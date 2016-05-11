import string
import re
import os
import math
import operator
from collections import Counter
from pprint import pprint
from stemming.porter import stem
import pickle

def build_queries():
    
    file = "query_desc.51-100.short.txt"
    with open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\"+file) as f:
        queries = f.readlines()
    q_dict = dict()

    for query in queries:    
        new_query = query.split('   ')
        li_query_terms=[]
        for m in re.finditer(r'\w+(\.?\w+)*', new_query[1].lower()):
                li_query_terms.append(m.group(0))
        edit_query = ' '.join(map(str, li_query_terms[3:]))
        q_dict[new_query[0][:-1]] = edit_query

    fin_query=''
    query_dict=dict()
    for key in q_dict.keys():    
        fin_query=' '.join([stem(word) for word in q_dict[key].split()])    
        query_dict[key]=fin_query
    return query_dict


def okapi_TF(tf, dlen, avg_dlen):
    return (tf/(tf+0.5+(1.5*(dlen/avg_dlen))))

def okapi_BM25(tf,dlen,avg_dlen,df,tf_query,corpus):
    k2=400
    k1=1.2
    b=0.75
    log_fn=math.log((corpus+0.5)/(df+0.5))
    mid_fn=(tf+k1*tf)/(tf+k1*((1-b)+(b*dlen/avg_dlen)))
    query_fn=(tf_query+(k2*tf_query))/(tf_query+k2)
    return log_fn*mid_fn*query_fn

def unigram_laplace(tf,dlen,vocab):
    return math.log((tf+1)/(dlen+vocab))

def get_results(term):

    term_id=token_hash[term]
    value=catalog[term_id]  
    offsets = value.split(',')
    start = int(offsets[0])
    length = int(offsets[1])
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Indexer_withStemming",'r')
    file.seek(start,0)
    content = file.read(length)
    file.close()
    opn=content.index('{')+1
    cls=content.index('}')
    content =content[opn:cls]
    dblks=content.split(';')
    inv_doc_hash = dict()
    doc_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemming", "rb" ) )
    for key,value in doc_hash.items():
        inv_doc_hash[value[0]]=key
    dblocks_hash=dict()
    for dblk in dblks:
        doc_id, posns = dblk.split(':')
        posns = posns.lstrip()
        opn = posns.index('(')+1
        end = posns.index(')')
        posns = posns[opn:end]
        posns = posns.split(',')
        doc_no = inv_doc_hash[int(doc_id)]
        dblocks_hash[doc_no] = posns
    return dblocks_hash

# loading hashtables
doc_dlen_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemming", "rb" ) )
token_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemming", "rb" ) )
catalog=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Catalog_withStemming", "rb" ) )
vocab=len(token_hash.keys())
## Finding average corpus length
total_dlen=0
for tup in doc_dlen_hash.values():
    total_dlen+=tup[1]
corpus = len(doc_dlen_hash.keys())
avg_dlen=total_dlen/corpus

query_dict=build_queries()
tf_idf_output=''
bm25_output=''
unigram_laplace_output=''
for queryNo,query in query_dict.items():
    tf_idf_score = dict()
    bm25_score = dict()
    unigram_laplace_score = dict()
    term_counts = Counter(query.split())
    for term in query.split():
        print (term)
        dblocks_hash = dict()
        dblocks_hash = get_results(term)
        doc_hash=dict()
        df = len(dblocks_hash.keys())
        tf_query=term_counts[term]
        
        for doc_no,value in dblocks_hash.items():
            doc_id = doc_dlen_hash[doc_no][0]
            tf = len(value)  
            doc_hash[doc_id] = doc_no  
            dlen = doc_dlen_hash[doc_no][1]   
            
            if doc_no in tf_idf_score.keys():
                tf_idf_score[doc_no]+=okapi_TF(tf, dlen, avg_dlen)*(math.log((corpus/df)))
            else:
                tf_idf_score[doc_no]=okapi_TF(tf, dlen, avg_dlen)*(math.log((corpus/df)))
            
            if doc_no in bm25_score.keys(): 
                bm25_score[doc_no]+=okapi_BM25(tf,dlen,avg_dlen,df,tf_query,corpus)
            else:
                bm25_score[doc_no]=okapi_BM25(tf,dlen,avg_dlen,df,tf_query,corpus)                

        for doc_no,value in doc_dlen_hash.items(): 
            ltf = 0
            dlen = value[1]
            if doc_no in dblocks_hash.keys():
                ltf = len(dblocks_hash[doc_no])                     
            
            if doc_no in unigram_laplace_score.keys(): 
                unigram_laplace_score[doc_no]+=unigram_laplace(ltf,dlen,vocab)
            else:
                unigram_laplace_score[doc_no]=unigram_laplace(ltf,dlen,vocab)
            
    
    sorted_tf_idf_score = sorted(tf_idf_score.items(), key=operator.itemgetter(1))
    sorted_tf_idf_score.reverse()
    final_tf_idf_score=sorted_tf_idf_score[:1000]
    rank=1    
    for ele in final_tf_idf_score:
        tf_idf_output+=str(queryNo)+' Q0'+ele[0]+str(rank)+' '+str(ele[1])+' Exp\n'
        rank+=1
    
    sorted_bm25_score = sorted(bm25_score.items(), key=operator.itemgetter(1))
    sorted_bm25_score.reverse()
    final_bm25_score=sorted_bm25_score[:1000]
    rank=1    
    for ele in final_bm25_score:
        bm25_output+=str(queryNo)+' Q0'+ele[0]+str(rank)+' '+str(ele[1])+' Exp\n'
        rank+=1
    
    sorted_unigram_laplace = sorted(unigram_laplace_score.items(), key=operator.itemgetter(1))
    sorted_unigram_laplace.reverse()
    final_unigram_laplace=sorted_unigram_laplace[:1000]
    rank=1    
    for ele in final_unigram_laplace:
        unigram_laplace_output+=str(queryNo)+' Q0'+ele[0]+str(rank)+' '+str(ele[1])+' Exp\n'
        rank+=1

file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\TF_IDF_Scores_withStemming.txt",'a')
file.write(tf_idf_output)
file.close()
file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Okapi_BM_25_Scores_withStemming.txt",'a')
file.write(bm25_output)
file.close()
file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Unigram_Laplace_Scores_withStemming.txt",'a')
file.write(unigram_laplace_output)
file.close()
