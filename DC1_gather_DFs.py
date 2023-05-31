import pandas as pd

# def extract_all_project_id():
#     all_pj_id = pd.read_csv("..\datasets\datasetsProjects_LastUpdate_Id.csv")                         
#     return all_pj_id

def extract_ce_project_id_rows(number_of_ce_csv_file):  
    final_res = pd.DataFrame(columns=['ChangeEventReason',	
                                      'DateCreated','Description','EventType','ProjectId','ROMAmount','Scope','Status'])
    for i in range(number_of_ce_csv_file):
        print(i+1)
        ce_df = pd.read_csv("D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\datasets\ChangeEvents_"+str(i+1)+".csv")
        ce_df=ce_df[['ChangeEventReason','DateCreated','Description','EventType','ProjectId','ROMAmount','Scope','Status']]
        res = ce_df
        final_res = pd.concat([final_res,res])
    return final_res

def extract_co_project_id_rows(number_of_co_csv_file): 
    final_res = pd.DataFrame()
    for i in range(number_of_co_csv_file):
        print(i+1)
        co_df = pd.read_csv("D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\datasets\ChangeOrders_"+str(i+1)+".csv",on_bad_lines='skip')
        res = co_df
        final_res = pd.concat([final_res,res])
    return final_res

def run_the_code(number_of_ce_files,number_of_co_files):
    print("///////// ce //////////")
    ce_retrieving = extract_ce_project_id_rows(number_of_ce_files)
    print(len(ce_retrieving))
    print("////// saving to csv /////////")
    ce_retrieving.to_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\1_ce_retrieving.csv',index=False)
    print("///////// co //////////")
    co_retrieving = extract_co_project_id_rows(number_of_co_files)
    print(len(co_retrieving))
    co_retrieving.to_csv(r'D:\Concordia\Master_Of_Science\Dataset_aedo_june_2022\Text_Mining\allprojects\1_co_retrieving.csv',index=False)
    print("////// finished  /////////")
    return(ce_retrieving,co_retrieving)
ch_ev_df,ch_or_df=run_the_code(5,9)

ch_or_df.describe()
