# -*- coding: utf-8 -*-
"""
Created on Mon Dec  5 17:15:05 2022

@author: barlettm

pipeline to transform docs into conll files (compatibles with inception)
https://pypi.org/project/spacy-conll/


USAGE :
When the ConllFormatter is added to a spaCy pipeline, it adds CoNLL properties for Token, sentence Span and Doc. Note that arbitrary Span's are not included and do not receive these properties.

On all three of these levels, two custom properties are exposed by default, ._.conll and its string representation ._.conll_str. However, if you have pandas installed, then ._.conll_pd will be added automatically, too!

._.conll: raw CoNLL format

in Token: a dictionary containing all the expected CoNLL fields as keys and the parsed properties as values.
in sentence Span: a list of its tokens' ._.conll dictionaries (list of dictionaries).
in a Doc: a list of its sentences' ._.conll lists (list of list of dictionaries).
._.conll_str: string representation of the CoNLL format

in Token: tab-separated representation of the contents of the CoNLL fields ending with a newline.
in sentence Span: the expected CoNLL format where each row represents a token. When ConllFormatter(include_headers=True) is used, two header lines are included as well, as per the CoNLL format.
in Doc: all its sentences' ._.conll_str combined and separated by new lines.
._.conll_pd: pandas representation of the CoNLL format

in Token: a Series representation of this token's CoNLL properties.
in sentence Span: a DataFrame representation of this sentence, with the CoNLL names as column headers.
in Doc: a concatenation of its sentences' DataFrame's, leading to a new a DataFrame whose index is reset.
You can use spacy_conll in your own Python code as a custom pipeline component, or you can use the built-in command-line script which offers typically needed functionality. See the following section for more.


https://pypi.org/project/spacy-udpipe/

v1. doc.
uploaded on inception with CoNLL-U error : 
Error while uploading document [NORM_CE1_20_out_clean_ann.txt]: 
    IOException: Invalid file format. Line needs to have 10 tab-separated fields, but it has 1: [1 

format to achieve - CoNLL-U
"The CoNLL-U format format targets dependency parsing. 
Columns are tab-separated. 
Sentences are separated by a blank new line." (inception doc)                                                                                                                                                            
"""
import glob
import os
import spacy
import time
#from spacy_conll import init_parser

today= time.strftime("%d-%m-%Y")

def write_dir(save_path):  
    if os.path.exists(save_path) == False:
        os.mkdir(save_path)
    else:
        pass

def write_conll(save_path, file, content):
    file = os.path.basename(file)
    write_dir(save_path)
    file_out = file[:-10]+"ann"
    complete_file_name = os.path.join(save_path, file_out)
    with open(complete_file_name+".conllu","w",encoding="utf8", newline="\n") as file_out:
        file_out.write(content)


def convert_conllu(folder_in, folder_out):
    folder_in = glob.glob(folder_in+"/*.txt")
    for this_file in folder_in :
        with open (this_file, "r", encoding="utf8") as infile :
            texte = infile.read().strip()
        doc = nlp(texte)
        str_conll = doc._.conll_str
        str_conll = str_conll+"\n"
        write_conll(folder_out, this_file, str_conll)
    return folder_out


def create_outfile(folder_out, outfile) : 
    folder_out=glob.glob(folder_out+"/*.conllu")
    #outfile="./output_conll/corpus_notags_23_02.conllu"
    with open(outfile,"w",encoding="utf8") as corpus_out:
        cpt=0
        for filename in folder_out :
            #print(filename)
            with open(filename, "r", encoding="utf8") as infile:
                content = infile.read()
                corpus_out.write("#id "+os.path.basename(filename)[:-11]+"\n")
                corpus_out.write(content)
            cpt+=1
    print(cpt)
    return cpt



def main(folder_in, folder_out, outfile):
    write_dir(folder_out)
    output = convert_conllu(folder_in, folder_out)
    create_outfile(folder_out, outfile)
    return output


#charger modèles spacy et conll formatter en dernier
#TODO ajouter attributs au conllU
nlp = spacy.load("fr_core_news_lg")
nlp.add_pipe("conll_formatter", last=True)


#date="05-09-2023_12-03"
##corpus conll à générer - dossier input et dossier output

###manual
date="_11-10-2023"
folder_in = ["./CE2normlong_27-09-2023_predv2/"]

folder_out = [f"./conll/transformer_{today}"]
              

outfile = [f"./conll/transformer_{today}.conllu"]

print("Début")
for infolder, outfolder, outfile in zip(folder_in, folder_out, outfile):
    main(infolder, outfolder, outfile)
print("Terminé")

