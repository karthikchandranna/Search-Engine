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

    stop_file = open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\stoplist.txt",'r')
    stop_words = []

    for line in stop_file:
        line=line[:-1]
        stop_words.append(line)
    fin_query=''
    query_dict=dict()
    for key in q_dict.keys():    
        fin_query=' '.join([stem(word) for word in q_dict[key].split() if word not in stop_words])    
        query_dict[key]=fin_query
    return query_dict


def get_results(term):

    term_id=token_hash[term]
    value=catalog[term_id]  
    offsets = value.split(',')
    start = int(offsets[0])
    length = int(offsets[1])
    file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Indexer_withStemnStop",'r')
    file.seek(start,0)
    content = file.read(length)
    file.close()
    opn=content.index('{')+1
    cls=content.index('}')
    content =content[opn:cls]
    dblks=content.split(';')
    inv_doc_hash = dict()
    doc_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemnStop", "rb" ) )
    for key,value in doc_hash.items():
        inv_doc_hash[value[0]]=key
    dblocks_hash=dict()
    for dblk in dblks:
        doc_id, posns = dblk.split(':')
        posns = posns.lstrip()
        opn = posns.index('(')+1
        end = posns.index(')')
        posns = posns[opn:end]
        posns = [int(pos) for pos in posns.split(',')] 
        doc_no = inv_doc_hash[int(doc_id)]
        dblocks_hash[doc_no] = posns
    return dblocks_hash


# loading hashtables
doc_dlen_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Doc_Hash_withStemnStop", "rb" ) )
token_hash=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Token_Hash_withStemnStop", "rb" ) )
catalog=pickle.load( open( "C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Catalog_withStemnStop", "rb" ) )
vocab=len(token_hash.keys())
# Finding average corpus length
total_dlen=0
for tup in doc_dlen_hash.values():
    total_dlen+=tup[1]
corpus = len(doc_dlen_hash.keys())
avg_dlen=total_dlen/corpus
query_dict=build_queries()
output=''
for queryNo,query in query_dict.items():
    qry_hash = dict()
    score = dict()
    for term in query.split():
        if term not in qry_hash.keys():
            print (term)
            dblocks_hash = dict()
            dblocks_hash = get_results(term)        
            qry_hash[term] = dblocks_hash

    doc_term_cnt_hash = dict()
    for term,dblks in qry_hash.items():
        for doc_no in dblks.keys():
            if doc_no in doc_term_cnt_hash:
                doc_term_cnt_hash[doc_no]+=1
            else:
                doc_term_cnt_hash[doc_no]=1

    doc_pos_hash = dict()
    for doc_no,cnt in doc_term_cnt_hash.items():
        if cnt > 1:
            for term,dblks in qry_hash.items():
                if doc_no in dblks.keys():
                    if doc_no in doc_pos_hash.keys():
                        doc_pos_hash[doc_no].append(dblks[doc_no])
                    else:
                        doc_pos_hash[doc_no]=[dblks[doc_no]]

    score = dict()
    for doc_no, li_posns in doc_pos_hash.items():
        range_wnd = 9999999999999
        while (all(len(posns) > 0 for posns in li_posns)):
            range_lst = []
            for posns in li_posns:
                range_lst.append(posns[0])
            mini = min(range_lst)
            maxi = max(range_lst)
            range_wnd = min(range_wnd,(maxi-mini))
            for posns in li_posns:
                if posns[0] == mini:
                    del posns[0]
                    break        
        dlen = doc_dlen_hash[doc_no][1]
        no_of_terms = doc_term_cnt_hash[doc_no]
        score[doc_no] = ((1500 - range_wnd)*no_of_terms/(dlen + vocab))


    sorted_score = sorted(score.items(), key=operator.itemgetter(1))
    sorted_score.reverse()
    no_of_scores = len(sorted_score)
    if no_of_scores > 1000:
        final_score=sorted_score[:1000]
    else:
        final_score=sorted_score[:no_of_scores-1]
    rank=1    
    for ele in final_score:
        output+=str(queryNo)+' Q0'+ele[0]+str(rank)+' '+str(ele[1])+' Exp\n'
        rank+=1

file=open("C:\\Users\\hp\\Desktop\\IR_Documents\\AP_DATA\\HW2_Data\\Proximity_Scores.txt",'a')
file.write(output)
file.close()

        
