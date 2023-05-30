import pandas as pd
import re
import nltk
from nltk.corpus import stopwords
import string
import numpy as np
# nltk.download('omw-1.4')
# nltk.download('stopwords')
# nltk.download('wordnet')
from nltk.tokenize import word_tokenize

stemmer = nltk.wordnet.WordNetLemmatizer()


def clean_plural_to_singular(description):
    list_description = description.split(' ')
    final_list_description = []
    for i in list_description:
        final_list_description.append(stemmer.lemmatize(i))
    return ' '.join(final_list_description)

def clean_stopwords(description):
    unrelevant_word = ["ce","ai","nbsp","hellip"]
    perso_stopwords = stopwords.words('english')
    perso_stopwords = perso_stopwords + unrelevant_word
    list_description = description.split(' ')
    final_list = [i for i in list_description if i not in perso_stopwords]
    return ' '.join(final_list)

def clean_ce_empty_cells(file_df):
    file_df['Count_Duplicates'] = 0   
    file_df.replace(r'^\s*$', np.nan, regex=True) 
    file_df['OrganizationChangeName']= file_df.OrganizationChangeName.str.strip().replace('',np.nan)
    # file_df['OrganizationChangeName']= file_df.OrganizationChangeName.fillna(0)
    # file_df  = file_df.query("OrganizationChangeName != 0 and OrganizationChangeName != 'nan' and OrganizationChangeName != 'Nan' ")
    #Do not run this line unless going to remove records with missing Description
    file_df.reset_index()
    return file_df

def change_order_pd_df (ch_or_df):

    print("DF is now read...")
    punct = string.punctuation
    punct_w_hash = punct.replace('#','')
    print("Cleaning Descriptiom 1/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: str(x).lower())
    print("Cleaning Description 2/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: re.sub(re.compile('<.*?>'), '', x))
    print("Cleaning Description 3/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: re.sub(re.compile('nbsp'), '', x))
    print("Cleaning Description 4/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: x.translate(str.maketrans(punct_w_hash,'                               ')))
    print("Cleaning Description 5/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: re.sub('([!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~])', r' \1 ', x))
    print("Cleaning Description 6/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: re.sub('\s{2,}', ' ', x))
    print("Cleaning Description 7/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: re.sub('\\s+', ' ', x)) 
    print("Cleaning Description 8/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: clean_stopwords(x))
    print("Cleaning Description 9/12...")
    ch_or_df['OrganizationChangeName'] = ch_or_df['OrganizationChangeName'].apply(lambda x: clean_plural_to_singular(x))
    print("Cleaning Description 10/12...")
    ch_or_df["OrganizationChangeName"] = ch_or_df["OrganizationChangeName"].apply(lambda x: x.replace("# ","#"))
    print("Cleaning Description 11/12...")
    ch_or_df["OrganizationChangeName"] = ch_or_df["OrganizationChangeName"].apply(lambda x: x.replace("#0","#"))
    print("Cleaning Description 12/12...")
    ch_or_df["DateCreated"]=pd.to_datetime(ch_or_df["DateCreated"])
    print("Cleaning step DONE....")
                        
    return ch_or_df

def change_order_group_by_duplicates (ch_or_df,run_no):

    
    ch_or_df.rename(columns={"Status":"co_Status"},inplace=True)
    ch_or_df["DateCreated"]=ch_or_df["DateCreated"].apply(lambda x:str(x).split(" ")[0])
    ch_or_df["DateCreated"]=pd.to_datetime(ch_or_df["DateCreated"],format="%Y-%m-%d")
    ch_or_df["co_Status"]=ch_or_df["co_Status"].replace({"approved":"3_approved","draft":"2_draft","proceeding":"2_proceeding"})
    ch_or_df=ch_or_df.sort_values(by=['OrganizationChangeName'],ascending=True,na_position="last").reset_index(drop=True)
    if run_no==1:
        ch_or_df = ch_or_df.groupby(['ProjectId', 'Amount','DateCreated',"Type","OrganizationChangeName"],as_index=False).agg({ 
     'Count_Duplicates':'count',"co_Status":"max","ChangeOrdersId":"last","ChangeOrdersInstanceId":"last","DateLastModified":"last","DateLastReviewed":"last","OrganizationChangeId":"last"})
        ch_or_df['Count_Duplicates'] = ch_or_df['Count_Duplicates'] - 1
    else:
        print("2nd duplicates round")
        ch_or_df = ch_or_df.groupby(['ProjectId', 'Amount','DateCreated',"Type","OrganizationChangeName"],as_index=False).agg({ 
      'Count_Duplicates':'sum',"co_Status":"max","ChangeOrdersId":"last","ChangeOrdersInstanceId":"last","DateLastModified":"last","DateLastReviewed":"last","OrganizationChangeId":"last"})

    print("Duplicates step DONE....")
    return ch_or_df
def change_order_group_by_pairs (ch_or_df):
    ch_or_df['Count_Pairs'] = 0
    ch_or_df['ABS_Amount'] = abs(ch_or_df['Amount'])  
    ch_or_df=ch_or_df.sort_values(by=['OrganizationChangeName'],ascending=True,na_position="last").reset_index(drop=True)
    # duplicates_attributes_df["Count_Duplicate"]=duplicates_attributes_df.groupby(["ProjectId","Amount",'DateCreated'],as_index=False)["ProjectId"].transform("count")
    ch_or_df=ch_or_df.groupby(["ProjectId","ABS_Amount",'DateCreated',"Type","OrganizationChangeName"],as_index=False).agg({ 
     'Count_Duplicates':'sum','Count_Pairs':'count',"Amount":"sum","co_Status":"last","ChangeOrdersId":"last","ChangeOrdersInstanceId":"last","DateLastModified":"last","DateLastReviewed":"last","OrganizationChangeId":"last"})
    # duplicates_attributes_df['Count_Pairs'] = duplicates_attributes_df['Count_Duplicates_Pairs']-duplicates_attributes_df["Count_Duplicate"]
    # duplicates_attributes_df.loc[duplicates_attributes_df["Count_Pairs"]>0,"Amount"]=0
    ch_or_df['Count_Pairs']=ch_or_df['Count_Pairs']-1
    print("Pairs step DONE....")
    return ch_or_df   
def change_order_with_count_duplicates_and_pairs_to_csv ():
    ch_or_df = pd.read_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\1_co_retrieving.csv')
    ch_or_df = clean_ce_empty_cells(ch_or_df)
    ch_or_df = change_order_group_by_duplicates(ch_or_df,1)
    ch_or_df = change_order_pd_df(ch_or_df)  
    ch_or_df = change_order_group_by_duplicates(ch_or_df,2)
    ch_or_df = change_order_group_by_pairs (ch_or_df)
    ch_or_df.to_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\2_co_without_duplicates.csv',index=False)
    return ch_or_df

    


test = change_order_with_count_duplicates_and_pairs_to_csv ()





