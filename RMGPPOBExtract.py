import os #this module provides a portable way of using operating system dependent functionality
import pandas as pd #library providing high-performance, easy-to-use data structures and data analysis tools
import numpy as np #fundamental package for high-level mathematical functions
import xlrd #library for developers to extract data from Microsoft Excel spreadsheet files
from tqdm import tqdm # to show progress in some log loops
from IPython.core.display import HTML # to print some formatted HTML in jupyter notebook
import datetime
import ExcelExtraction as EE
import sys


def main(argv):

	pd.set_option('mode.chained_assignment', None)

	print("#######################################################")
	print("#######################START###########################")
	print()


	print("#######################-01-############################\n")

	try:
		path = argv[0]
	except IndexError:
		path = input("Please enter the folder path name where the RAW OBs are: \n")

	os.chdir(path)


	count = 0
	for root, dirs, files in os.walk(path):
		for file_ in files:
			count += 1
	
	print ("Total number of available files : ", count)


	all_files = EE.extract_all_files(path)


	sheets = []
	for key, df in enumerate(all_files):
		if df.sheet.values[0] not in sheets:
			sheets.append(df.sheet.values[0])

	print ("Available sheet names in files : \n", sheets)

	layout_dfs = []
	for key, df in enumerate(all_files):
		if df.sheet.values[0]=='Layout.':
			layout_dfs.append(df.copy())


	print("Available OB file (Layout.) count ",len(layout_dfs))
	input("\nPress Enter To Proceed")


	ob_info_df = pd.DataFrame(columns=['Buyer_OB','Style_OB','Order_number_OB' ,'path', 'file', 'sheet'] )

	for key, df in enumerate(layout_dfs):
		buyer_ = df.loc[2, 'Unnamed: 1']
		style_ = df.loc[3, 'Unnamed: 1']
		order_ = df.loc[4, 'Unnamed: 1']
		ob_info_df.loc[len(ob_info_df)] = pd.Series( index = ['Buyer_OB','Style_OB','Order_number_OB', 'path', 'file', 'sheet'] ,
													  data=[buyer_, style_, order_, df.path[0], df.file[0], df.sheet[0]]) 


	print("#######################-02-############################\n")
	print("Styles information count: \n",ob_info_df.count())



	process_info_df = pd.DataFrame(columns=['Process_OB','Machine_type_OB','Individual_SMV_OB',
											'Standard_target_OB','Operator_OB','Helper_OB',
											'Actual_target_OB','path', 'file', 'sheet'])

	for key, df in enumerate(layout_dfs):
		
		try:
			start_row = df[df[df.columns[1]]=='Operation'].index[0]
		except IndexError:
			start_row = df[df[df.columns[0]]=='No.'].index[0]
			print(df.file[0])
			  
		end_row   = df[df[df.columns[1]]=='TOTAL'].index[0]
		
		process_df = df.loc[start_row+2:end_row-1]
		
		process_df.rename(columns={
			
			'Unnamed: 1':'Process_OB',
			'Unnamed: 2':'Machine_type_OB',
			'Unnamed: 3':'Individual_SMV_OB',
			'Unnamed: 4':'Standard_target_OB',
			'Unnamed: 5':'Operator_OB',
			'Unnamed: 6':'Helper_OB', 
			'Unnamed: 7':'Actual_target_OB',
			
		}, inplace=True)
		process_df = process_df[process_info_df.columns]
		
		process_info_df = pd.concat([process_info_df, process_df], ignore_index=True)

	input("\nPress Enter To Proceed")
	print("#######################-03-############################\n")
	print("Process information count: \n",process_info_df.count())
	input("\nPress Enter To Proceed")

	ob_df = pd.merge(ob_info_df, process_info_df, on=['path', 'file', 'sheet'])

	#print (len(ob_info_df), len(process_info_df), len(ob_df))


	ob_df[ pd.isnull(ob_df.Process_OB) ].count()


	ob_df = ob_df[pd.notnull(ob_df.Process_OB)]



	ob_df.drop(['sheet'], axis=1, inplace=True)

	#print (len(ob_df))




	ob_df.Operator_OB = ob_df.Operator_OB.replace(np.nan, 0)
	ob_df.Helper_OB = ob_df.Helper_OB.replace(np.nan, 0)

	print("#######################-04-############################\n")
	print("Operation Breakdown Dataset information: \n")
	print(ob_df.info())
	input("\nPress Enter To Proceed")




	originalDF = ob_df.copy()




	print("#######################-05-############################\n")
	print("Total Observation Count: {}, After Dropping Duplicates: {}".format(len(ob_df),
		len(ob_df.drop_duplicates(['Style_OB', 'Process_OB']))))


	ob_df.drop_duplicates(['Style_OB', 'Process_OB'],inplace=True)

	ob_df.reset_index(drop=True,inplace=True)

	ob_df['Style_OB'] = ob_df['Style_OB'].str.lower()


	ob_df= ob_df[pd.notnull(ob_df.Individual_SMV_OB)]



	# ob_df.columns

	# print (len(ob_df.groupby(by=["Buyer_OB",'Style_OB','path', 'file'])))

	# print (len(ob_df.groupby(by=['Style_OB','path', 'file'])))


	for key, data in ob_df.groupby(by=['Style_OB','path', 'file']):
		ob_df.loc[data.index,"Total_SMV"] = data.Individual_SMV_OB.sum()
		ob_df.loc[data.index,"operation_id"] = pd.Series([i for i in range(len(data)+1) if i != 0],data.index)


# 	ob_df[pd.isnull(ob_df["operation_id"])]

	ob_df["factory_code"] = "201901"


# 	ob_df.columns


	ob_df.columns = [col.lower().replace(" ","_") for col in ob_df.columns if pd.notnull(col)]


	bkupDF = ob_df.copy()

	# We wilL now deal with processes which has more than one manpower.


	bkupDF['operator_ob'].unique()

	bkupDF['helper_ob'].unique()



	bkupDF['allocated_mp'] = pd.Series([np.ceil(np.sum([i,j])) for i,j in zip(bkupDF['operator_ob'],bkupDF['helper_ob'])],index=bkupDF.index)



	print("\nManpower unique values: {} (should be <10)\n".format(bkupDF['allocated_mp'].unique()))


	print("Copying observations for multiple manpower")

# 	len(bkupDF[bkupDF.allocated_mp == 0])

	bkupDF.drop(bkupDF[bkupDF.allocated_mp == 0].index,inplace=True)
	bkupDF.reset_index(drop=True,inplace=True)



	letters = ['A','B','C','D','E','F','G','H',"I","J","K","L"]
	DF = pd.DataFrame(columns=bkupDF.columns)
	DF['op_no_ipa'] = np.nan
	for row in tqdm(bkupDF.index):
		if int(bkupDF.loc[row,'allocated_mp'])>1:   
			for i in range(int(bkupDF.loc[row,'allocated_mp'])):
				values = bkupDF.loc[row]
				values["op_no_ipa"] = str(int(bkupDF.loc[row,"operation_id"]))+letters[i]
				DF.loc[len(DF)] = values
		else:
			values = bkupDF.loc[row]
			values["op_no_ipa"] = str(int(bkupDF.loc[row,"operation_id"]))
			DF.loc[len(DF)] = values



	DF.reset_index(drop=True,inplace=True)



	DF.style_ob = DF.style_ob.str.strip()



	cols = ['operation_id','factory_code','buyer_ob','style_ob','order_number_ob','process_ob','machine_type_ob',
			'individual_smv_ob','operator_ob','helper_ob','actual_target_ob','total_smv','op_no_ipa','standard_target_ob',
			'path', 'file']



	DF = DF[cols]



	col_dict = {'buyer_ob':"Buyer_OB",'style_ob':"Style_OB",'order_number_ob':"Order_number_OB",'process_ob':"Process_OB",
				'machine_type_ob':"Machine_type_OB",'individual_smv_ob':'Individual_SMV_OB','operator_ob':"Operator_OB",
				'helper_ob':'Helper_OB','actual_target_ob':"Actual_target_OB",'total_smv':"ttl_smv"}



	DF.rename(columns=col_dict,inplace=True)

	print()
	print ("Extracted dataset length: {}\n".format(len(DF)))

# Check if the styles have already been extracted, if not we need to append those with the master OB file.

# reading in the main ob file


	main_ob = pd.read_csv(r"../../Done/MasterOB.csv")



	print ("Master OB Length: {}".format(len(main_ob)))


	#checking if the column names are same
	if len([item for item in DF.columns if item not in main_ob.columns.unique()])>1:
		print ("ERROR")
		print ("New OB dataset has columns that is not present in master dataset")
		sys.exit()



	#checking if the styles are already in the main OB master data
	style_list = [item for item in DF['Style_OB'].unique() if item in main_ob["Style_OB"].unique()]



	# This list should be empty, if not we need to look for the styles in the main database first (workbench ob table) and if 
	# they are there and have the same total SVM's, we will remove the files from the raw folder we are currently working on and extract 
	# again till the list is empty. But if the SVM's are different then we need to talk to the collection team to be sure whether they 
	# are new styles or not.​
	

	if len(style_list)>1:
		
		print()
		print("Following styles are already extracted before: \n {}".format(style_list))
		
		style_dict_db = {}
		for key,data in DF[DF['Style_OB'].isin(style_list)].groupby(by='Style_OB'):
			style_dict_db[key] = float(str(data.ttl_smv.unique()[0])[:5])



		style_dict_maindb = {}
		for key,data in main_ob[main_ob['Style_OB'].isin(style_list)].groupby(by='Style_OB'):
			style_dict_maindb[key] = float(str(data.ttl_smv.unique()[0])[:5])


		print()
		print("\nStyles with their Total SMVs")
		print()
		print("New Dataset:   ", style_dict_db)
		print("Master Dataset:", style_dict_maindb)



		del_style_list = []
		for style_ in style_list:
			if style_dict_db[style_] == style_dict_maindb[style_]:
				del_style_list.append(style_)
			else:
				new_style = style_+'-'+''.join(str(datetime.datetime.today().date()).split('-'))
				indices = DF[DF.Style_OB == style_].index
				DF.loc[indices,'Style_OB'] = new_style


		print("\n So {} styles will be removed\n".format(del_style_list))


		del_indices = DF[DF.Style_OB.isin(del_style_list)].index
		del_path = originalDF[originalDF.Style_OB.str.lower().str.strip().isin(del_style_list)].path.unique()[0]
		del_files = originalDF[originalDF.Style_OB.str.lower().str.strip().isin(del_style_list)].file.unique()


		len(DF)


		DF.drop(del_indices,inplace=True)
		DF.reset_index(drop=True,inplace=True)



		len(DF)


		for file_ in del_files:
			os.remove(del_path+"\\"+file_)

		print("Deleted files from the folder are following:\n\n {}".format(del_files))


	input("\nPress Enter To Proceed")
	print("#######################-06-############################\n")
	# Updatting Master File

	len(main_ob)

	#appending new file with the master file
	main_ob = main_ob.append(DF, ignore_index=True)

	len(main_ob)

	# Making Output Files


	today_ = ''.join(str(datetime.datetime.today().date()).split('-'))
	DF.to_csv("../../Done/1301 OB updated "+today_+".csv", index=False,encoding='utf-8')
	DF.to_pickle("../../Done/1301 OB updated "+today_)
	main_ob.to_csv("../../Done/MasterOB "+today_+".csv",index=False)
	main_ob.to_csv("../../Done/MasterOB.csv",index=False)


	print("#######################################################\n")
	print("Output files have been generated\n")
	print("Upload only the latest extracted file, not the main file (MasterOB) we just made\n")
	print("#######################END############################\n")

	return

if __name__=="__main__":
	main(sys.argv[1:])