import string
import re
import os
from pprint import pprint
import pickle
import time
from stemming.porter import stem

def createDocAndTokenHashes_NoStemnStop():

    print("Creating Document Hash and Token Hash with no stemming or stopping....")
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    doc_id=1
    token_increment = 1
    doc_hash = dict()
    token_hash = dict()
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)        
        if docs:
            for doc in docs:
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                texts=''.join(map(str, li_texts))
                dlen=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token = m.group(0)
                    dlen+=1
                    if token not in token_hash.keys():
                        token_hash[token] = token_increment
                        token_increment+=1
                doc_hash[doc_no]=(doc_id,dlen)
                doc_id+=1
    print("Docs= "+str(doc_id-1))
    print("Tokens with no stemming or stopping= "+str(token_increment-1))
    pickle.dump( doc_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_noStemnStop", "wb" ) )
    pickle.dump( token_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_noStemnStop", "wb" ) )

def createPartialIndexes_NoStemnStop():
    
    doc_itr=0
    catalog={}
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\Partial_Indexer_noStemnStop","a")
    file.close()
    doc_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_noStemnStop", "rb" ) )
    token_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_noStemnStop", "rb" ) )
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    dblock_hash={}
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)
        if docs:
            for doc in docs:
                if (doc_itr%1000 == 0 and doc_itr!=0):
                    #Dump to index
                    indexfile_content = ''
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_noStemnStop",'a')
                    indexfile_offset = file.tell()
                    file.close()
                    
                    for token_id in dblock_hash.keys():
                        token_content = str(token_id)+"{"
                        for doc_id in dblock_hash[token_id].keys():
                            token_content += str(doc_id)+": ("+ dblock_hash[token_id][doc_id] + ");"
                        token_content += "}"                        
                        if token_id in catalog.keys():
                            tkn_ent_list = catalog[token_id]
                            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
                            catalog[token_id] = tkn_ent_list
                        else:
                            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
                        indexfile_content += token_content
                        indexfile_offset+=len(token_content)
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_noStemnStop",'a')
                    file.write(indexfile_content)
                    file.close()
                    dblock_hash={}
                    print("Dumped " +str(doc_itr)+ " non-stemmed and non-stopped Documents")
                    
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                doc_id = doc_hash[doc_no][0]
                texts=''.join(map(str, li_texts))
                texts=texts.lower()
                tkn_cnt=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token = m.group(0)                    
                    tkn_cnt+=1
                    token_id=token_hash[token]
                    if token_id in dblock_hash.keys():
                        if doc_id in dblock_hash[token_id].keys():
                            dblock_hash[token_id][doc_id]+= ","+str(tkn_cnt)
                        else:
                            dblock_hash[token_id][doc_id]=str(tkn_cnt)
                    else:
                        dblock_hash[token_id]={}
                        dblock_hash[token_id][doc_id]=str(tkn_cnt)
                
                doc_itr+=1
    #Dump the last chunk of dblocks to index
    indexfile_content = ''
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_noStemnStop",'a')
    indexfile_offset = file.tell()
    file.close()
    
    for token_id in dblock_hash.keys():
        token_content = str(token_id)+"{"
        for doc_no in dblock_hash[token_id].keys():
            token_content += str(doc_no)+": ("+ dblock_hash[token_id][doc_no] + ");"
        token_content += "}"                        
        if token_id in catalog.keys():
            tkn_ent_list = catalog[token_id]
            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
            catalog[token_id] = tkn_ent_list
        else:
            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
        indexfile_content += token_content
        indexfile_offset+=len(token_content)
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_noStemnStop",'a')
    file.write(indexfile_content)
    file.close()
    pickle.dump( catalog, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog_noStemnStop", "wb" ) )
    print("Dumped All non-stemmed and non-stopped Documents")

######################### With no Stop Words ################################

def createDocAndTokenHashes_WithStopping():

    print("Creating Document Hash and Token Hash  with Stop Words removed....")
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    doc_id=1
    token_increment = 1
    doc_hash = dict()
    token_hash = dict()
    stop_file = open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\stoplist.txt",'r')
    stop_words = []
    for line in stop_file:
        line=line[:-1]
        stop_words.append(line)
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)        
        if docs:
            for doc in docs:
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                texts=''.join(map(str, li_texts))
                dlen=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token = m.group(0)
                    if token not in stop_words:
                        dlen+=1
                        if token not in token_hash.keys():
                            token_hash[token] = token_increment
                            token_increment+=1
                doc_hash[doc_no]=(doc_id,dlen)
                doc_id+=1
    print("Docs= "+str(doc_id-1))
    print("Tokens with no stop words= "+str(token_increment-1))
    pickle.dump( doc_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStopping", "wb" ) )
    pickle.dump( token_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStopping", "wb" ) )

def createPartialIndexes_WithStopping():
    
    doc_itr=0
    catalog={}
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\Partial_Indexer_withStopping","a")
    file.close()
    doc_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStopping", "rb" ) )
    token_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStopping", "rb" ) )
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    dblock_hash={}
    stop_file = open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\stoplist.txt",'r')
    stop_words = []
    for line in stop_file:
        line=line[:-1]
        stop_words.append(line)
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)
        if docs:
            for doc in docs:
                if (doc_itr%1000 == 0 and doc_itr!=0):
                    #Dump to index
                    indexfile_content = ''
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStopping",'a')
                    indexfile_offset = file.tell()
                    file.close()
                    
                    for token_id in dblock_hash.keys():
                        token_content = str(token_id)+"{"
                        for doc_id in dblock_hash[token_id].keys():
                            token_content += str(doc_id)+": ("+ dblock_hash[token_id][doc_id] + ");"
                        token_content += "}"                        
                        if token_id in catalog.keys():
                            tkn_ent_list = catalog[token_id]
                            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
                            catalog[token_id] = tkn_ent_list
                        else:
                            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
                        indexfile_content += token_content
                        indexfile_offset+=len(token_content)
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStopping",'a')
                    file.write(indexfile_content)
                    file.close()
                    dblock_hash={}
                    print("Dumped " +str(doc_itr)+ " Documents with Stop Words removed")
                    
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                doc_id = doc_hash[doc_no][0]
                texts=''.join(map(str, li_texts))
                texts=texts.lower()
                tkn_cnt=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token = m.group(0)
                    if token not in stop_words:
                        tkn_cnt+=1
                        token_id=token_hash[token]
                        if token_id in dblock_hash.keys():
                            if doc_id in dblock_hash[token_id].keys():
                                dblock_hash[token_id][doc_id]+= ","+str(tkn_cnt)
                            else:
                                dblock_hash[token_id][doc_id]=str(tkn_cnt)
                        else:
                            dblock_hash[token_id]={}
                            dblock_hash[token_id][doc_id]=str(tkn_cnt)
                
                doc_itr+=1
    #Dump the last chunk of dblocks to index
    indexfile_content = ''
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStopping",'a')
    indexfile_offset = file.tell()
    file.close()
    
    for token_id in dblock_hash.keys():
        token_content = str(token_id)+"{"
        for doc_no in dblock_hash[token_id].keys():
            token_content += str(doc_no)+": ("+ dblock_hash[token_id][doc_no] + ");"
        token_content += "}"                        
        if token_id in catalog.keys():
            tkn_ent_list = catalog[token_id]
            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
            catalog[token_id] = tkn_ent_list
        else:
            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
        indexfile_content += token_content
        indexfile_offset+=len(token_content)
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStopping",'a')
    file.write(indexfile_content)
    file.close()
    pickle.dump( catalog, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog_withStopping", "wb" ) )
    print("Dumped All Documents with Stop Words removed")

########################### With Stemming ###################################

def createDocAndTokenHashes_WithStemming():

    print("Creating Document Hash and Token Hash with stemming....")
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    doc_id=1
    token_increment = 1
    doc_hash = dict()
    token_hash = dict()
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)        
        if docs:
            for doc in docs:
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                texts=''.join(map(str, li_texts))
                dlen=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token_noStem = m.group(0)
                    token= stem(token_noStem)
                    dlen+=1
                    if token not in token_hash.keys():
                        token_hash[token] = token_increment
                        token_increment+=1
                doc_hash[doc_no]=(doc_id,dlen)
                doc_id+=1
    print("Docs= "+str(doc_id-1))
    print("Tokens with stemming= "+str(token_increment-1))
    pickle.dump( doc_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemming", "wb" ) )
    pickle.dump( token_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemming", "wb" ) )

def createPartialIndexes_WithStemming():
    
    doc_itr=0
    catalog={}
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\Partial_Indexer_withStemming","a")
    file.close()
    doc_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemming", "rb" ) )
    token_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemming", "rb" ) )
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    dblock_hash={}
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)
        if docs:
            for doc in docs:
                if (doc_itr%1000 == 0 and doc_itr!=0):
                    #Dump to index
                    indexfile_content = ''
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemming",'a')
                    indexfile_offset = file.tell()
                    file.close()
                    
                    for token_id in dblock_hash.keys():
                        token_content = str(token_id)+"{"
                        for doc_id in dblock_hash[token_id].keys():
                            token_content += str(doc_id)+": ("+ dblock_hash[token_id][doc_id] + ");"
                        token_content += "}"                        
                        if token_id in catalog.keys():
                            tkn_ent_list = catalog[token_id]
                            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
                            catalog[token_id] = tkn_ent_list
                        else:
                            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
                        indexfile_content += token_content
                        indexfile_offset+=len(token_content)
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemming",'a')
                    file.write(indexfile_content)
                    file.close()
                    dblock_hash={}
                    print("Dumped " +str(doc_itr)+ " stemmed Documents")
                    
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                doc_id = doc_hash[doc_no][0]
                texts=''.join(map(str, li_texts))
                texts=texts.lower()
                tkn_cnt=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token_noStem = m.group(0)
                    token= stem(token_noStem)
                    tkn_cnt+=1
                    token_id=token_hash[token]
                    if token_id in dblock_hash.keys():
                        if doc_id in dblock_hash[token_id].keys():
                            dblock_hash[token_id][doc_id]+= ","+str(tkn_cnt)
                        else:
                            dblock_hash[token_id][doc_id]=str(tkn_cnt)
                    else:
                        dblock_hash[token_id]={}
                        dblock_hash[token_id][doc_id]=str(tkn_cnt)
                
                doc_itr+=1
    #Dump the last chunk of dblocks to index
    indexfile_content = ''
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemming",'a')
    indexfile_offset = file.tell()
    file.close()
    
    for token_id in dblock_hash.keys():
        token_content = str(token_id)+"{"
        for doc_no in dblock_hash[token_id].keys():
            token_content += str(doc_no)+": ("+ dblock_hash[token_id][doc_no] + ");"
        token_content += "}"                        
        if token_id in catalog.keys():
            tkn_ent_list = catalog[token_id]
            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
            catalog[token_id] = tkn_ent_list
        else:
            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
        indexfile_content += token_content
        indexfile_offset+=len(token_content)
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemming",'a')
    file.write(indexfile_content)
    file.close()
    pickle.dump( catalog, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog_withStemming", "wb" ) )
    print("Dumped All stemmed Documents")

####################### With Stemming and Stopping ##########################

def createDocAndTokenHashes_WithStemnStop():

    print("Creating Document Hash and Token Hash after Stemming and with Stop Words removed....")
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    doc_id=1
    token_increment = 1
    doc_hash = dict()
    token_hash = dict()
    stop_file = open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\stoplist.txt",'r')
    stop_words = []
    for line in stop_file:
        line=line[:-1]
        stop_words.append(line)
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)        
        if docs:
            for doc in docs:
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                texts=''.join(map(str, li_texts))
                dlen=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token_noStem = m.group(0)                    
                    if token_noStem not in stop_words:
                        token= stem(token_noStem)
                        dlen+=1
                        if token not in token_hash.keys():
                            token_hash[token] = token_increment
                            token_increment+=1
                doc_hash[doc_no]=(doc_id,dlen)
                doc_id+=1
    print("Docs= "+str(doc_id-1))
    print("Tokens after Stemming and with no stop words= "+str(token_increment-1))
    pickle.dump( doc_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemnStop", "wb" ) )
    pickle.dump( token_hash, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemnStop", "wb" ) )

def createPartialIndexes_WithStemnStop():
    
    doc_itr=0
    catalog={}
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\Partial_Indexer_withStemnStop","a")
    file.close()
    doc_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemnStop", "rb" ) )
    token_hash = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemnStop", "rb" ) )
    files = os.listdir("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection")
    dblock_hash={}
    stop_file = open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\stoplist.txt",'r')
    stop_words = []
    for line in stop_file:
        line=line[:-1]
        stop_words.append(line)
    for file in files:
        with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\ap89_collection\\"+file) as f:
            doc_file = f.read()
        docs=re.findall(r'<DOC>(.*?)</DOC>',doc_file,re.DOTALL)
        if docs:
            for doc in docs:
                if (doc_itr%1000 == 0 and doc_itr!=0):
                    #Dump to index
                    indexfile_content = ''
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemnStop",'a')
                    indexfile_offset = file.tell()
                    file.close()
                    
                    for token_id in dblock_hash.keys():
                        token_content = str(token_id)+"{"
                        for doc_id in dblock_hash[token_id].keys():
                            token_content += str(doc_id)+": ("+ dblock_hash[token_id][doc_id] + ");"
                        token_content += "}"                        
                        if token_id in catalog.keys():
                            tkn_ent_list = catalog[token_id]
                            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
                            catalog[token_id] = tkn_ent_list
                        else:
                            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
                        indexfile_content += token_content
                        indexfile_offset+=len(token_content)
                    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemnStop",'a')
                    file.write(indexfile_content)
                    file.close()
                    dblock_hash={}
                    print("Dumped " +str(doc_itr)+ " Documents after Stemming and with Stop Words removed")
                    
                li_doc_no=re.findall(r'<DOCNO>(.*?)</DOCNO>',doc)
                li_texts=re.findall(r'<TEXT>(.*?)</TEXT>',doc,re.DOTALL)
                doc_no= ''.join(map(str, li_doc_no))
                doc_id = doc_hash[doc_no][0]
                texts=''.join(map(str, li_texts))
                texts=texts.lower()
                tkn_cnt=0
                for m in re.finditer(r'\w+(\.?\w+)*', texts.lower()):
                    token_noStem = m.group(0)
                    if token_noStem not in stop_words:
                        token= stem(token_noStem)
                        tkn_cnt+=1
                        token_id=token_hash[token]
                        if token_id in dblock_hash.keys():
                            if doc_id in dblock_hash[token_id].keys():
                                dblock_hash[token_id][doc_id]+= ","+str(tkn_cnt)
                            else:
                                dblock_hash[token_id][doc_id]=str(tkn_cnt)
                        else:
                            dblock_hash[token_id]={}
                            dblock_hash[token_id][doc_id]=str(tkn_cnt)
                
                doc_itr+=1
    #Dump the last chunk of dblocks to index
    indexfile_content = ''
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemnStop",'a')
    indexfile_offset = file.tell()
    file.close()
    
    for token_id in dblock_hash.keys():
        token_content = str(token_id)+"{"
        for doc_no in dblock_hash[token_id].keys():
            token_content += str(doc_no)+": ("+ dblock_hash[token_id][doc_no] + ");"
        token_content += "}"                        
        if token_id in catalog.keys():
            tkn_ent_list = catalog[token_id]
            tkn_ent_list.append((str(indexfile_offset)+","+str(len(token_content))))
            catalog[token_id] = tkn_ent_list
        else:
            catalog[token_id]=[(str(indexfile_offset)+","+str(len(token_content)))]
        indexfile_content += token_content
        indexfile_offset+=len(token_content)
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer_withStemnStop",'a')
    file.write(indexfile_content)
    file.close()
    pickle.dump( catalog, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog_withStemnStop", "wb" ) )
    print("Dumped All Documents after Stemming and with Stop Words removed")

#############################################################################
                                                                                
def createIndex(index_type):

    print("Creating Complete Inverted Index in HardDisk....")
    catalog = pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog"+index_type, "rb" ) )
    file_partial=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer"+index_type,"r")
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Indexer"+index_type,"a")
    final_catalog = {}
    final_index_offset = 0
    for token_id in catalog.keys():
        dblocks=[]
        final_entry=""
        for offset in catalog[token_id]:
            start, length = offset.split(',')
            file_partial.seek(int(start),0)        
            content = file_partial.read(int(length))
            content=content.split(';')
            content=content[:-1]
            tkn_len = len(str(token_id))
            content[0]=content[0][tkn_len+1:]
            dblocks+=content
        final_entry=str(token_id)+"{"+';'.join(dblocks)+"}"
        final_catalog[token_id]= str(final_index_offset)+","+str(len(final_entry))
        final_index_offset+=len(final_entry)
        file.write(final_entry)

    file.close()
    file_partial.close()
    pickle.dump( final_catalog, open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Catalog"+index_type, "wb" ) )
    os.remove("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Dummy_Catalog"+index_type)
    os.remove("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Partial_Indexer"+index_type)

start_time = time.clock()
#### With no Stemming or Stopping ###
createDocAndTokenHashes_NoStemnStop()
createPartialIndexes_NoStemnStop()
createIndex("_noStemnStop")
########### With Stopping ###########
createDocAndTokenHashes_WithStopping()
createPartialIndexes_WithStopping()
createIndex("_withStopping")
########### With Stemming ###########
createDocAndTokenHashes_WithStemming()
createPartialIndexes_WithStemming()
createIndex("_withStemming")
##### With Stemming and Stopping ####
createDocAndTokenHashes_WithStemnStop()
createPartialIndexes_WithStemnStop()
createIndex("_withStemnStop")
print("--- %s seconds ---" % (time.clock() - start_time))

