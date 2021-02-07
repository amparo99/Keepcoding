#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler,OneHotEncoder

from imblearn.pipeline import Pipeline
from imblearn.over_sampling import SMOTE
from imblearn.under_sampling import RandomUnderSampler

from datetime import datetime


# In[2]:


'''def process_data(data):
    #keep data from Madrid
    data.drop(data[data['City']!='Madrid'].index, inplace = True)
    
    #keep selected features + price
    features = set(['Neighbourhood Group Cleansed','Bathrooms',
            'Property Type', 'Room Type', 'Accommodates', 'Price'])
    data = data[features]
    
    #clean outliers in "propety type"
    properties = data["Property Type"].value_counts().keys().tolist()
    counts = data["Property Type"].value_counts().tolist()
    for (p, count) in zip(properties, counts):
        if count < 20:
            idxs = data[data["Property Type"] == p].index
            data.drop(idxs, inplace=True)
    
    #clean outliers in "accommodates"
    data = data[data['Accommodates']<=8]
    
    #clean outliers in "bathrooms"
    bathrooms = data["Bathrooms"].value_counts().keys().tolist()
    counts = data["Bathrooms"].value_counts().tolist()
    for (bath, count) in zip(bathrooms, counts):
        if count < 25:
            idxs = data[data["Bathrooms"] == bath].index
            data.drop(idxs, inplace=True)
        
    #clean outliers in "price"
    data = data[data['Price']<200]
    data = data[data['Price']>0] #to make sure we don't have nonpositive values
    
    #fill nans
    categorical_features = data.columns[data.dtypes == 'object']
    numerical_features = set(data.columns[(data.dtypes == 'int64') | (data.dtypes =='float64')]) - set(['Price'])

    for col in categorical_features:
        data[col].fillna(data[col].mode()[0], inplace=True)
    
    for col in numerical_features:
        data[col].fillna(data[col].mean(), inplace=True)
    
        
    return data'''
def process_data(data,tipo,mean_map={}):
    #drop cols decided in the analysis of the train_dataset:
    drop_cols = ['ID', 'Scrape ID', 'Last Scraped', 'Name','Listing Url','Thumbnail Url', 'Medium Url',
                'Picture Url', 'XL Picture Url','Host URL','Host Thumbnail Url', 'Host Picture Url',
                'Summary', 'Space','Description','Neighborhood Overview', 'Notes','Transit','Access',
                'Interaction','House Rules','Host About','Calendar Updated', 'Calendar last Scraped', 
                'First Review', 'Last Review','Weekly Price', 'Monthly Price','Host ID','Host Name',
                'Host Location','Host Neighbourhood','Calculated host listings count',
                'Host Listings Count', 'Host Total Listings Count','Experiences Offered','Geolocation',
                'Country Code','Country','Smart Location','Market','State','Street',
                'Neighbourhood', 'Neighbourhood Cleansed','Zipcode', 
                'Host Acceptance Rate', 'Square Feet', 'Security Deposit','Cleaning Fee', 'Has Availability',
                'License', 'Jurisdiction Names','Beds','Availability 30','Availability 60','Availability 90',
                'Review Scores Value','Number of Reviews','Review Scores Rating','Review Scores Communication']
    data.drop(drop_cols, axis = 1, inplace = True)
    
    #filter for prices lower than 200, deleting the outliers:
    data = data[data['Price']<200]
    
    #Filter to keep only apartments in Madrid:
    data.drop(data[data['City']!='Madrid'].index, inplace = True)
    data.drop(['City'], axis = 1, inplace = True)
    
    #Edit variables: Amenities, Features, Host Verifications
    data['Features'] = data['Features'].apply(lambda x: len(str(x).split(',')))
    data['Amenities'].fillna(0, inplace = True)
    data['Amenities'] = data['Amenities'].apply(lambda x: len(str(x).split(',')) if x!=0 else 0)
    data['Host Verifications'].fillna(0, inplace = True)
    data['Host Verifications'] = data['Host Verifications'].apply(lambda x: len(str(x).split(',')) if x!=0 else 0)
    
    #New variable bed_bath_rooms:
    data['bed_bath_rooms']   = data['Bedrooms']*data['Bathrooms']
    
    #delete errors
    data.drop(data[data['Bedrooms']==0].index, inplace = True)
    data.drop(data[data['bed_bath_rooms']==0].index, inplace = True)
    data.drop(data[data['Reviews per Month']==0].index, inplace = True)
    
    
    #outliers in accommodates:
    data = data[data['Accommodates']<8]
    
    #tratar los campos vacíos:
    data.dropna(subset=['Host Since'], inplace = True)
    
    data["Host Response Time"].fillna(data["Host Response Time"].mode()[0], inplace=True)
    data = data.fillna(data.mean())
    
    #transformations
    data = data[data['bed_bath_rooms']>0] #para asegurarnos que no peta el log si hay un error con un numero negativo
    data = data[data['Reviews per Month']>0]
    data['bed_bath_rooms'] = data['bed_bath_rooms'].apply(lambda x: np.log(x))
    data['Reviews per Month'] = data['Reviews per Month'].apply(lambda x: np.log(x))
    
    #host since:
    data['Host Since'] = data['Host Since'].apply(lambda x: datetime.strptime(str(x),'%Y-%m-%d'))
    data['year'] = data['Host Since'].apply(lambda x: 2020 - x.year)
    data.drop('Host Since', axis = 1, inplace = True)
    
    #variables categóricas
    cols = data.columns[data.dtypes == 'object']
        
    if tipo == 'train':
        mean_map = {}
        for col in cols:
            mean = data.groupby(col)['Price'].mean()
            data[col] = data[col].map(mean)    
            mean_map[col] = mean
            data[col].fillna(data[col].mode()[0], inplace=True)
        return data, mean_map
    
    elif tipo == 'test':
        for col in cols:
            data[col] = data[col].map(mean_map[col])
            #para evitar quedarnos con campos categóricos vacios si nos aparece un nuevo valor para alguna variable
            data[col].fillna(data[col].mode()[0], inplace=True)
        return data
    


# In[3]:


def convert_classification(data):
    cheap = data[data['Price'] <= float(30)]
    affordable = data[(data['Price']>float(30)) & (data['Price']<=float(75))]
    expensive = data[data['Price'] > float(80)]
    
    cheap['label'] = 'cheap'
    affordable['label'] = 'affordable'
    expensive['label'] = 'expensive'

    df = pd.concat([cheap,affordable,expensive])
    df.drop(['Price'],axis=1, inplace=True)    
    
    return df


# In[4]:


def encoder(data):
    categorical_features = data.columns[data.dtypes == 'object']
    for col in categorical_features:
        data = pd.concat([data,pd.get_dummies(data[col], prefix=col)],axis=1)
        data.drop([col],axis=1, inplace=True)
    return data


# In[5]:


def fix_encoder(train,test):
    # Get missing columns in the training test
    missing_cols = set( train.columns ) - set( test.columns )
    # Add a missing column in test set with default value equal to 0
    for c in missing_cols:
        test[c] = 0
    # Ensure the order of column in the test set is in the same order than in train set
    test = test[train.columns]
    
    return train, test


# In[6]:


def balance_dataset(data,n):
    data = data.groupby(['label']).apply(lambda _df: _df.sample(n=n, random_state=1, replace = False))
    data.reset_index(level=0, drop=True, inplace = True)
    return data


# In[7]:


def split(train, test):
    objective_var = 'Price' #['label_cheap','label_affordable','label_expensive']
    y_train = train[objective_var]
    y_test = test[objective_var] 
    
    X_train = train.drop(objective_var, axis = 1)
    X_test = test.drop(objective_var, axis = 1)
    

    return X_train, y_train, X_test, y_test


# In[8]:


def scale(X_train, X_test):

    scaler = MinMaxScaler().fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)
    
    return X_train, X_test


# In[9]:


def label_encoder(df):
    label = pd.get_dummies(df['label'], prefix='label')
    df = pd.concat([df, label], axis=1)
    # drop old label
    df.drop(['label'], axis=1, inplace=True)
    
    return df


# In[15]:


def reduce(train,test):
    train = train.sample(2000, replace = False)
    test = test.sample(300, replace = False)
    return train,test


# In[16]:


def get_data_ready(train, test):
    train, mean_map = process_data(train, 'train')
    test = process_data(test, 'test', mean_map)

    #train = convert_classification(train)
    #test = convert_classification(test)

    #balance dataset
    #n = min(train.groupby('label').get_group('cheap').shape[0],train.groupby('label').get_group('affordable').shape[0],train.groupby('label').get_group('expensive').shape[0])
    #n = 500
    #train = balance_dataset(train,n)

    train,test = reduce(train,test)
    # label -> one-hot encoding
    #train = label_encoder(train)
    #test = label_encoder(test)
        
    #one-hot encoder for categorical variables
    #train = encoder(train)
    #test = encoder(test)
    #train,test = fix_encoder(train,test)
    
    #Divide in X/y
    X_train, y_train, X_test, y_test = split(train,test)
    
    #and scale
    X_train, X_test = scale(X_train, X_test)

    return X_train, y_train, X_test, y_test


# In[ ]:




