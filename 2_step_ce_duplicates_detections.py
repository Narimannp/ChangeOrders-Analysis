import pandas as pd
import re 
import nltk
import numpy as np
from nltk.corpus import stopwords
# nltk.download('omw-1.4')
import string
from nltk.tokenize import word_tokenize
# nltk.download('punkt')

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
    file_df.replace(r'^\s*$', np.nan, regex=True) 
    file_df['Description']= file_df.Description.str.strip().replace('',np.nan)
    file_df['Description']= file_df.Description.fillna(0)
    # file_df = file_df.query("Description != 0 and Description != 'nan' and Description != 'Nan' ")
    # Dont run this section unless going to remove record with empty description
    file_df.reset_index()
    return file_df

def change_event_pd_df_cleaned(ch_ev_df):
    punct = string.punctuation
    punct_w_hash = punct.replace('#','')
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: str(x).lower())
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: re.sub(re.compile('<.*?>'), '', x))# remove html tag
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: x.translate(str.maketrans(punct_w_hash,'                               ')))
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: re.sub('([!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~])', r' \1 ', x)) # space between words and punctuation to match plurals
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: re.sub('\\s+', ' ', x)) # remove more than 2 spaces/ newlines ect ...
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: clean_stopwords(x)) # remove ce and ai
    ch_ev_df['Description'] = ch_ev_df['Description'].apply(lambda x: clean_plural_to_singular(x))
    ch_ev_df["Description"] = ch_ev_df["Description"].apply(lambda x: x.replace("# ","#"))
    ch_ev_df["Description"] = ch_ev_df["Description"].apply(lambda x: x.replace("#0","#"))                        
    return ch_ev_df

def change_event_group_by_duplicates ():
    ch_ev_df = pd.read_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\1_ce_retrieving.csv')
    ch_ev_df['Count_Duplicates'] = 0            
    ch_ev_df["DateCreated"]=ch_ev_df["DateCreated"].apply(lambda x:x.split(" ")[0])
    ch_ev_df["DateCreated"]=pd.to_datetime(ch_ev_df["DateCreated"],format="%Y-%m-%d")
    # ch_ev_df["Description"]=np.where(ch_ev_df["Description"].isna()," ",ch_ev_df["Description"])
    ch_ev_df["Status"]=ch_ev_df["Status"].replace({"approved":"4_approved","rejected":"4_rejected",
    "claim   not approved":"4_claim_not_approved","disputed":"4_disputed","rejected":"4_rejected", "draft":"3_draft",
    "void":"3_void","no cost":"3_no_cost","quoted":"2_quoted","not quoted":"1_not_quated","authorized":"0_authorized"})
    ch_ev_df["Scope"]=ch_ev_df["Scope"].replace({"tbd":0,"in_scope":1,"out_of_scope":2})
    pd.to_numeric(ch_ev_df["Scope"])
    ch_ev_df=ch_ev_df.sort_values(by=['Description'],ascending=True,na_position="last").reset_index(drop=True)
    ch_ev_df = ch_ev_df.groupby(['ROMAmount', 'DateCreated',"ProjectId","Description"],as_index=False).agg({ 
     'Count_Duplicates':'count',"ChangeEventReason":"first","EventType":"first","Scope":"max",'EventType':"first","Status":"first"})
    
    ch_ev_df['Count_Duplicates'] = ch_ev_df['Count_Duplicates'] - 1
    return ch_ev_df

def change_events_group_by_pairs (ch_ev_df):
    ch_ev_df['Count_Pairs'] = 0
    ch_ev_df['ABS_ROMAmount'] = abs(ch_ev_df['ROMAmount'])  
    ch_ev_df=ch_ev_df.sort_values(by=['Description'],ascending=True,na_position="last").reset_index(drop=True)
    # duplicates_attributes_df["Count_Duplicate"]=duplicates_attributes_df.groupby(["ProjectId","Amount",'DateCreated'],as_index=False)["ProjectId"].transform("count")
    ch_ev_df=ch_ev_df.groupby(["ProjectId","ABS_ROMAmount",'DateCreated',"Description","Status"],as_index=False).agg({ 
     'Count_Duplicates':'sum','Count_Pairs':'count',"ChangeEventReason":"first","EventType":"first","Scope":"max","ROMAmount":"sum"})
    # duplicates_attributes_df['Count_Pairs'] = duplicates_attributes_df['Count_Duplicates_Pairs']-duplicates_attributes_df["Count_Duplicate"]
    # duplicates_attributes_df.loc[duplicates_attributes_df["Count_Pairs"]>0,"Amount"]=0
    ch_ev_df['Count_Pairs']=ch_ev_df['Count_Pairs']-1
    return ch_ev_df   
def longuest_common_substring(ce_text, co_text):
    set_of_words_ce=set(str(ce_text).split())
    set_of_words_co=set(str(co_text).split())
    longuest_substring = set_of_words_ce & set_of_words_co
    return ' '.join(longuest_substring)

def change_event_diff_status(ch_ev_df):
    ch_ev_df=ch_ev_df
def change_event_with_count_duplicates_to_csv():
    ch_ev_df = change_event_group_by_duplicates()
    ch_ev_df = change_event_pd_df_cleaned(ch_ev_df)
    ch_ev_df = change_events_group_by_pairs (ch_ev_df)
    ch_ev_df.to_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\2_ce_retrieving.csv',index=False)
    return ch_ev_df

def run_the_code():
    ch_ev_df_duplicates = change_event_with_count_duplicates_to_csv()
    print("//////////////////////")
    print(len(ch_ev_df_duplicates))
    ch_ev_df_duplicates_cleaned = clean_ce_empty_cells(ch_ev_df_duplicates)
    print("//////////////////////")
    print(len(ch_ev_df_duplicates_cleaned))
    ch_ev_df_duplicates_cleaned["ce_description_id"] =  0
    for i in range(len(ch_ev_df_duplicates_cleaned)):
        ch_ev_df_duplicates_cleaned["ce_description_id"].iloc[i] = i 
    ch_ev_df_duplicates_cleaned.to_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\3_ce_without_duplicates_cleaned.csv',index=False) 
    return ch_ev_df_duplicates_cleaned


def count():
    ch_ev_df = pd.read_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\3_ce_without_duplicates_cleaned.csv')
    count_token = 0
    for i in ch_ev_df["Description"] :
        count_token += len(word_tokenize(i))
    return [count_token,len(ch_ev_df)]
        
    


test = run_the_code()

count_token = count()
print("No of descriptions: ", count_token[1])
print("Token no: ",count_token[0])