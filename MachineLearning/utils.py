#!/usr/bin/env python
# coding: utf-8

# In[6]:


import numpy as np
import pandas as pd

from datetime import datetime


# In[3]:


def proces_data(data,tipo,mean_map={}):
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
    


# In[ ]:




