# Full-text Search

This project is a text retrieval mechanism which searches query terms in a collection of 90,000 documents from the American Press(AP) 1989 corpus and ranks the documents based on various retrieval models.

Tools: Python
 
•	The program parses the corpus and does stopping, stemming and tokenizing of the terms and creates partial inverted lists for all terms in a single pass through the AP corpus. As each partial list is filled, it is appended to the end of a single large index file. When all documents in the corpus have been processed, the program runs through the large file one term at a time and merges the partial lists for each term and create a final inverted index of the format (termID, DF, TTF,(DocID1,TF, Pos1,Pos2....PosN),(DocID2,TF,Pos1,Pos2....PosN)). This was greatly accelerated by keeping a list of the positions of all the partial lists for each term in some secondary data structure or file(catalog). Also a final catalog having the termID, offset in the inverted index, length of the term data is maintained for random access of the term details in the inverted index.
•	Ranked top 1000 relevant documents for a given query using vector space models like Okapi TF, TF/IDF, BM25 and language models like Unigram LM with Laplace smoothing and Jelinek-Mercer smoothing for Unigram based search.  
•	Ranked top 1000 relevant documents for a given query using Proximity search for ngram based search.  
