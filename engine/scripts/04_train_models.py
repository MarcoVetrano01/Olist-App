import os
import re
import joblib 
import pandas as pd
import torch
from torch import nn
import torch.optim as optim
from torch.utils.data import DataLoader
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm

import nltk
import spacy as sp
from nltk.corpus import stopwords

# --- CONFIGURAZIONI PERCORSI (DOCKER) ---
INPUT_FILE = '/app/exports/reviews.csv'
MODELS_EXPORT_DIR = '/app/exports/models'

# Sottocartelle per i singoli modelli
SKLEARN_DIR = os.path.join(MODELS_EXPORT_DIR, 'sklearn')
TORCH_DIR = os.path.join(MODELS_EXPORT_DIR, 'torch_gru')
BERT_DIR = os.path.join(MODELS_EXPORT_DIR, 'bert')

# Creiamo preventivamente TUTTE le cartelle (Evita la morte del processo dopo ore di training)
os.makedirs(SKLEARN_DIR, exist_ok=True)
os.makedirs(TORCH_DIR, exist_ok=True)
os.makedirs(BERT_DIR, exist_ok=True)

# --- INIZIALIZZAZIONE STRUMENTI NLP ---
print("⚙️ Inizializzazione librerie NLP...")
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords', quiet=True)

nlp = sp.load('pt_core_news_sm')

def parse_reviews(review):
    if not isinstance(review, str):
        return ""
    review = review.lower()
    reviews_clean = re.sub(r"<.*?>", "", review) # toglie i tag html
    reviews_clean = re.sub(r'[^a-zA-Z0-9\s]', '', reviews_clean) # toglie i caratteri speciali
    swords = set(stopwords.words('portuguese'))
    text = [word.lemma_ for word in nlp(reviews_clean) if word.text not in swords] # toglie le stopwords
    return ' '.join(text)

# --- CLASSI PYTORCH (Invariate, erano buone) ---
class Dataset(torch.utils.data.Dataset):
    def __init__(self, df, tokenizer, max_len):
        self.data = df
        self.reviews = self.data['review_comment_message'].values
        self.labels = self.data['is_positive'].values
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        review = str(self.reviews[idx])
        label = self.labels[idx]
        dato_tokenizzato = self.tokenizer(review, 
            padding='max_length',
            max_length=self.max_len,
            truncation=True,
            return_tensors='pt')
        input_ids = dato_tokenizzato['input_ids'].squeeze()
        etichetta_tensor = torch.tensor(label, dtype=torch.float32)
        return input_ids, etichetta_tensor

class ModelloSentimento_GRU(nn.Module):
    def __init__(self, grandezza_vocabolario, dim_embedding, dim_nascosta):
        super().__init__()
        self.embedding = nn.Embedding(grandezza_vocabolario, dim_embedding)
        self.gru = nn.GRU(dim_embedding, dim_nascosta, batch_first=True, bidirectional=True)
        self.norm = nn.LayerNorm(dim_nascosta * 2) 
        self.dropout = nn.Dropout(0.3)
        self.lineare = nn.Linear(dim_nascosta * 2, 1)

    def forward(self, x):
        vettori = self.embedding(x)
        output_gru, ultimo_hidden = self.gru(vettori)
        # Concateniamo l'ultimo stato hidden delle direzioni forward e backward
        riassunto = torch.cat((ultimo_hidden[-2], ultimo_hidden[-1]), dim=1)
        riassunto_normalizzato = self.norm(riassunto)
        riassunto_pulito = self.dropout(riassunto_normalizzato)
        previsione = self.lineare(riassunto_pulito)
        return previsione

class Dataset_Transfer_Learning(torch.utils.data.Dataset):
    def __init__(self, df, tokenizer, max_len):
        self.data = df
        self.reviews = self.data['review_comment_message'].values
        self.labels = self.data['is_positive'].values
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.data)
    
    def __getitem__(self, idx):
        review = str(self.reviews[idx])
        label = self.labels[idx]
        dato_tokenizzato = self.tokenizer(review, 
            padding='max_length',
            max_length=self.max_len,
            truncation=True,
            return_tensors='pt')
        input_ids = dato_tokenizzato['input_ids'].squeeze()
        attention_mask = dato_tokenizzato['attention_mask'].squeeze()
        etichetta_tensor = torch.tensor(label, dtype=torch.float32)
        return input_ids, attention_mask, etichetta_tensor


def train_logistic_regression(df):
    print("\n" + "="*50)
    print("TRAINING LOGISTIC REGRESSION (Baseline)")
    print("="*50)
    
    reviews = df['cleaned_reviews']
    is_positive = df['is_positive']

    vectorizer = TfidfVectorizer(input='content', max_features=5000)
    xtrain, xtest, ytrain, ytest = train_test_split(reviews, is_positive, test_size=0.2, random_state=42)
    
    xtrain_tokenized = vectorizer.fit_transform(xtrain)
    xtest_tokenized = vectorizer.transform(xtest)

    model = LogisticRegression(class_weight='balanced')
    model.fit(xtrain_tokenized, ytrain)
    
    accuracy = model.score(xtest_tokenized, ytest)
    print(f"✅ Accuracy Scikit-Learn: {accuracy * 100:.2f}%")

    # Salvataggio sicuro
    joblib.dump(model, os.path.join(SKLEARN_DIR, 'logistic_regression.pkl'))
    joblib.dump(vectorizer, os.path.join(SKLEARN_DIR, 'tfidf_vectorizer.pkl')) # SERVE PER L'INFERENZA IN STREAMLIT!
    print("💾 Modello e Vectorizer salvati con successo!")

def train_gru(df):
    print("\n" + "="*50)
    print("🚀 TRAINING CUSTOM GRU PYTORCH")
    print("="*50)
    
    tokenizer = AutoTokenizer.from_pretrained('neuralmind/bert-base-portuguese-cased')
    df_train, df_test = train_test_split(df, test_size=0.2, random_state=42)
    
    dataset_train = Dataset(df_train, tokenizer, max_len=50)
    dataset_test = Dataset(df_test, tokenizer, max_len=50)

    dataloader_train = DataLoader(dataset_train, batch_size=32, shuffle=True)
    dataloader_test = DataLoader(dataset_test, batch_size=32, shuffle=False)
    
    VOCAB_SIZE = tokenizer.vocab_size
    EMBEDDING_DIM = 50
    HIDDEN_DIM = 64
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    modello = ModelloSentimento_GRU(VOCAB_SIZE, EMBEDDING_DIM, HIDDEN_DIM).to(device)
    criterio = nn.BCEWithLogitsLoss()
    ottimizzatore = optim.Adam(modello.parameters(), lr=0.001)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(ottimizzatore, mode='min', factor=0.1, patience=2)
    epoche = 5

    for epoca in range(epoche):
        modello.train() 
        loss_totale_epoca = 0
        
        for batch_testi, batch_etichette in tqdm(dataloader_train, desc=f"Epoca {epoca+1}/{epoche}"):
            batch_testi, batch_etichette = batch_testi.to(device), batch_etichette.to(device)
            ottimizzatore.zero_grad()
            previsioni_grezze = modello(batch_testi).squeeze()
            loss = criterio(previsioni_grezze, batch_etichette)
            loss.backward()
            ottimizzatore.step()
            loss_totale_epoca += loss.item()
            
        modello.eval()
        loss_totale_test = 0
        previsioni_corrette = 0
        totale_recensioni = 0

        with torch.no_grad():
            for batch_testi, batch_etichette in dataloader_test:
                batch_testi, batch_etichette = batch_testi.to(device), batch_etichette.to(device)
                previsioni_grezze = modello(batch_testi).squeeze()
                loss = criterio(previsioni_grezze, batch_etichette)
                loss_totale_test += loss.item()
                
                probabilita = torch.sigmoid(previsioni_grezze)
                previsioni_nette = (probabilita > 0.5).float()
                previsioni_corrette += (previsioni_nette == batch_etichette).sum().item()
                totale_recensioni += len(batch_etichette)

        test_loss_media = loss_totale_test / len(dataloader_test)
        scheduler.step(test_loss_media)
        accuracy = previsioni_corrette / totale_recensioni
        print(f"Test Loss: {test_loss_media:.4f} | Accuracy: {accuracy * 100:.2f}%")

    torch.save(modello.state_dict(), os.path.join(TORCH_DIR, 'modello_gru_pesi.pth'))
    print("Model successfully saved!")

def train_bert(df):
    print("\n" + "="*50)
    print("FINE-TUNING HUGGINGFACE BERT")
    print("="*50)
    
    tokenizer = AutoTokenizer.from_pretrained('neuralmind/bert-base-portuguese-cased')
    df_train, df_test = train_test_split(df, test_size=0.2, random_state=42)
    
    dataset_train = Dataset_Transfer_Learning(df_train, tokenizer, max_len=50)
    dataset_test = Dataset_Transfer_Learning(df_test, tokenizer, max_len=50)
    dataloader_train = DataLoader(dataset_train, batch_size=32, shuffle=True)
    dataloader_test = DataLoader(dataset_test, batch_size=32, shuffle=False)

    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"Using device: {device}")
    
    modello_bert = AutoModelForSequenceClassification.from_pretrained(
        'neuralmind/bert-base-portuguese-cased', 
        num_labels=1
    ).to(device)

    criterio = nn.BCEWithLogitsLoss() 
    ottimizzatore = optim.Adam(modello_bert.parameters(), lr=2e-5) 
    epoche = 3 

    for epoca in range(epoche):
        modello_bert.train() 
        loss_totale_epoca = 0
        
        for batch_testi, batch_attention_mask, batch_etichette in tqdm(dataloader_train, desc=f"Epoca BERT {epoca+1}/{epoche}"):
            batch_testi = batch_testi.to(device)
            batch_attention_mask = batch_attention_mask.to(device)
            batch_etichette = batch_etichette.to(device)
            
            ottimizzatore.zero_grad()
            output_bert = modello_bert(input_ids=batch_testi, attention_mask=batch_attention_mask) 
            previsioni = output_bert.logits.squeeze()
            
            loss = criterio(previsioni, batch_etichette)
            loss.backward()
            ottimizzatore.step()
            loss_totale_epoca += loss.item()
            
        modello_bert.eval()
        loss_totale_test = 0
        previsioni_corrette = 0
        totale_recensioni = 0

        with torch.no_grad():
            for batch_testi, batch_attention_mask, batch_etichette in dataloader_test:
                batch_testi = batch_testi.to(device)
                batch_attention_mask = batch_attention_mask.to(device)
                batch_etichette = batch_etichette.to(device)
                
                output_bert = modello_bert(input_ids=batch_testi, attention_mask=batch_attention_mask)
                previsioni = output_bert.logits.squeeze()
                loss = criterio(previsioni, batch_etichette)
                loss_totale_test += loss.item()
                
                probabilita = torch.sigmoid(previsioni)
                previsioni_nette = (probabilita > 0.5).float()
                previsioni_corrette += (previsioni_nette == batch_etichette).sum().item()
                totale_recensioni += len(batch_etichette)

        accuracy = previsioni_corrette / totale_recensioni
        print(f"Test Loss: {loss_totale_test / len(dataloader_test):.4f} | Accuracy: {accuracy * 100:.2f}%")

    modello_bert.save_pretrained(BERT_DIR)
    tokenizer.save_pretrained(BERT_DIR)
    print("Model successfully saved!")

if __name__ == '__main__':
    print("📥 Uploading dataset...")
    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} not found. Did you run 03_data_pipeline.py?")
        exit(1)
        
    df = pd.read_csv(INPUT_FILE)
    
    df = df.dropna(subset=['review_comment_message', 'is_positive'])

    print("Cleaning reviews with NLP...")
    tqdm.pandas(desc='Pulizia NLP')
    df['cleaned_reviews'] = df['review_comment_message'].progress_apply(parse_reviews)
    
    train_logistic_regression(df)
    
    
    train_gru(df)
    train_bert(df)
    
    print("\n🎉 Training completed!")