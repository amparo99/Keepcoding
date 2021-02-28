#!/usr/bin/env python
# coding: utf-8

# In[1]:


import json
import pandas as pd
import re
import string
import unidecode
import nltk
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from numpy import random


# In[2]:


def remove_accents(text):
    if text:
        unaccented_string = unidecode.unidecode(text)
        return unaccented_string
    return ""

def remove_punctuation_marks(text):
    if text:
        translator = str.maketrans('', '', string.punctuation)
        return text.translate(translator)
    return ""

def text_to_lower_case(text):
    if text:
        # Convertir a minusculas 
        # (aunque más adelante el tf-idf vectorizer lo hace automático, 
        #lo hacemos como paso del procesamiento del texto.
        return text.lower()
    return ""

def remove_emojis(text):
    if text:
        str_enc = text.encode(encoding = 'ascii', errors = 'ignore')
        return str_enc.decode(encoding = 'ascii', errors='ignore')
    return ""

def remove_multiple_whitespaces(text):
    if text:
        return re.sub(' +', ' ', text).strip()
    return ""

def remove_text_marks(text):
    if text:
        # Reemplazamos it's por its
        text = re.sub(r"\'","", text)
        # Reemplazamos *, ?, ... por espacios
        text = re.sub(r'[^\w]', " ", text)
        return text.strip()
    
    return ""

def remove_alone_numbers(text):
    if text:
        text = re.sub(r"\d"," ", text)
        return text
    
    return ""


# In[3]:


def clean_text(text):
    text = text_to_lower_case(text)
    text = remove_text_marks(text)
    text = remove_punctuation_marks(text)
    text = remove_accents(text)
    text = remove_emojis(text)
    text = remove_alone_numbers(text)
    text = remove_multiple_whitespaces(text)
    
    # Return
    return text


# In[4]:


def get_tokens(text):
    if isinstance(text, nltk.Text):
        tokens = text.tokens
    else:
        tokens = text.split(" ")
    
    return tokens

def remove_stopwords(text, 
                     language):
    # Import stopwords
    from nltk.corpus import stopwords
    
    stopwords_list = stopwords.words(language)
    
    tokens = get_tokens(text)
        
    cleaned_text = [tokens[i] for i in range(len(tokens)) if tokens[i] not in stopwords_list]
    
    if isinstance(text, nltk.Text):
        output = nltk.Text(cleaned_text)
    else:
        output = " ".join(cleaned_text)
    
    return output

def stem_text(text,
              language):
   
    from nltk.stem.snowball import SnowballStemmer
   
    stemmer = SnowballStemmer(language)
    
    tokens = get_tokens(text)
    
    stemmas = [stemmer.stem(word) for word in tokens]
    
    if isinstance(text, nltk.Text):
        output = nltk.Text(stemmas)
    else:
        output = " ".join(stemmas)
    
    return output

def standardize_text(text,
                     language):
    # Remove the stop words
    standardized_text = remove_stopwords(text, language)
    
    # Stemming
    standardized_text = stem_text(standardized_text, language)
    
    # Return
    return standardized_text


# In[6]:


def do_text_transformations(data: pd.DataFrame,
                            text_field: str,
                            language: str = "english"):
    # TODO: Clean the text
    data['Text_Clean'] = data[text_field].apply(lambda text: clean_text(text))
    idx = np.array(data[data.Text_Clean == ''].index)
    data.drop(idx, axis = 0, inplace = True)
    
    # TODO: Standardize the text
    data['Standardized_Text'] = data.Text_Clean.apply(lambda text: standardize_text(text, language))

    #data['Sentiment'] = data.overall.apply(lambda value: star_sentiment_matching(value))
    #data = data.drop(columns = ["overall"])
    
    #objective_var = 'Sentiment'
    #y = data[objective_var].values
    #X = data['Standardized_Text']
    
    return data


# In[ ]:




