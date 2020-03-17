#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on 24.04.2018

@author: fbockni
'''

import asyncio
import requests
import numpy as np
import pandas as pd
from pprint import pprint
from collections import defaultdict
from copy import deepcopy
import pickle
import os

def request_work_list(query=None, filters={}, params={}, max_results=0):
    
    default_params = {}
    default_params["mailto"] = "techscouting@arkentec.de"
    default_params["sort"] = "score"
    #default_params["sort"] = "issued"
    default_params["rows"] = 1000
    if max_results > 0 and max_results < default_params["rows"]:
        default_params["rows"] = max_results
    default_params["cursor"] = "*"
    
    req_params = {**default_params, **params}
    
    if query:
        if type(query) == str:
            req_params["query"] = query
        elif type(query) == dict:
            for k, v in query.items():
                if k.startswith("query."):
                    req_params[k] = v
                else:
                    req_params["query."+k] = v
    
    if filters:
        req_params["filter"] = ",".join(["{}:{}".format(k, v) for k, v in filters.items()])
        
    work_list = []
    
    while True:
        r = requests.get("https://api.crossref.org/works", params=req_params)

        r.raise_for_status()

        print("requesting", r.url)

        rJson = r.json()
        if rJson["status"] == "ok" and rJson["message-type"] == "work-list":
            msg = rJson["message"]
            total_results = msg["total-results"]
            new_items = msg["items"]
            work_list += new_items
            next_cursor = msg["next-cursor"]
            req_params["cursor"] = next_cursor
        else:
            print("error requesting", r.url)
            print(r.json)
        
        print("got {} of {} items".format(len(work_list), total_results))
        
        if max_results > 0:
            if len(work_list) >= max_results:
                work_list = work_list[:max_results]
                break
        
        if len(work_list) >= total_results:
            break
        
        if len(new_items) == 0:
            print("error: got 0 items. returning work list.")
            break
            
    
    return work_list


def request_journal_infos(issn, count_since_year):
    
    req_params = {}
    req_params["mailto"] = "techscouting@arkentec.de"
    
    r = requests.get("https://api.crossref.org/journals/{}".format(issn), params=req_params)
    
    r.raise_for_status()        

    print("requesting", r.url)

    rJson = r.json()
    if rJson["status"] == "ok" and rJson["message-type"] == "journal":
        msg = rJson["message"]
        journalTitle = msg["title"]
        publisher = msg["publisher"]
        dois_by_year = defaultdict(int)
        for year, count in msg["breakdowns"]["dois-by-issued-year"]:
            dois_by_year[year] = count
        
        count_total = sum([c for y,c in dois_by_year.items() if y >= count_since_year])
    
    return journalTitle, publisher, count_total


def request_work_list_from_issn(issn, filters={}, params={}, max_results=0, max_total_results=0):
    
    default_params = {}
    default_params["mailto"] = "techscouting@arkentec.de"
    
    
    default_params["sort"] = "score"
    #default_params["sort"] = "issued"
    default_params["rows"] = 1000
    if max_results > 0 and max_results < default_params["rows"]:
        default_params["rows"] = max_results
    default_params["cursor"] = "*"
    
    req_params = {**default_params, **params}
    
    if filters:
        req_params["filter"] = ",".join(["{}:{}".format(k, v) for k, v in filters.items()])
        
    work_list = []
    
    while True:
        r = requests.get("https://api.crossref.org/journals/{}/works".format(issn), params=req_params)
        
        r.raise_for_status()
        

        print("requesting", r.url)

        rJson = r.json()
        if rJson["status"] == "ok" and rJson["message-type"] == "work-list":
            msg = rJson["message"]
            total_results = msg["total-results"]
            new_items = msg["items"]
            work_list += new_items
            next_cursor = msg["next-cursor"]
            req_params["cursor"] = next_cursor
        else:
            print("error requesting", r.url)
            print(r.json)
        
        print("got {} of {} items".format(len(work_list), total_results))
        
        if max_results > 0:
            if len(work_list) >= max_results:
                work_list = work_list[:max_results]
                break
        
        if len(work_list) >= total_results:
            break
        
        if len(new_items) == 0:
            print("error: got 0 items. returning work list.")
            break
        
        if max_total_results > 0:
            if total_results > max_total_results:
                print("error: too many results ({}); max-cutoff: {}".format(total_results, max_total_results))
                return []
    
    return work_list


def request_work(doi, params={}):
    default_params = {}
    default_params["mailto"] = "techscouting@arkentec.de"
    
    req_params = {**default_params, **params}
    
    r = requests.get("https://api.crossref.org/works/"+doi, params=req_params)
    r.raise_for_status()

    #print("requesting", r.url)
    
    rJson = r.json()
    
    if rJson["status"] == "ok" and rJson["message-type"] == "work":
        msg = rJson["message"]
        #print(msg)
        return msg
    else:
        raise KeyError(doi)
    
def update_work_dict(work_dict, wl):
    for w in wl:
        doi = w["DOI"]
        work_dict[doi] = deepcopy(w)
    return work_dict

def get_reference_dois(work):
    result = []
    if "reference" in work:
        for ref in work["reference"]:
            if "DOI" in ref:
                result.append(ref["DOI"])
    return result

def get_ref_counts(work_dict):
    ref_counts = defaultdict(int)
    for work in work_dict.values():
        #print(work)
        work_refs = get_reference_dois(work)
        #print(work_refs)
        #break
        for doi in work_refs:
            ref_counts[doi] += 1
    return ref_counts

import concurrent.futures
import requests
import time

def request_work_async(i, doi, N, params={}):
    
    max_retries = 10
    if (i % 100) == 0:
        print(i, "/", N)
        #print(i, "start")
    for k in range(max_retries):
        
        time.sleep(1)
        try:
            msg = request_work(doi)
            break
        except requests.HTTPError as e:
            print("HTTPError", e)
            status_code = e.response.status_code
            if status_code == 429:
                #asyncio.sleep(5)
                time.sleep(5)
            elif status_code == 404:
                return doi
            else:
                return doi
        except requests.exceptions.SSLError as e:
            print("SSLError", e) 
            return doi
        except requests.exceptions.ProxyError as e:
            print("ProxyError", e) 
            return doi
            
        #asyncio.sleep(1)
        
    #print(i, "done")
    return msg


async def main_request_loop(dois, no_workers=20):

    items = []
    bad_dois = []
    N = len(dois)

    with concurrent.futures.ThreadPoolExecutor(max_workers=no_workers) as executor:

        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor, 
                request_work_async, 
                i,
                doi,
                N
            )
            for i, doi in enumerate(dois)
        ]    

        #print("loop done")

        i = 0

        for response in await asyncio.gather(*futures):
            #pprint (response)
            #print(i, "done")
            if response:
                if type(response) == str:
                    bad_dois.append(response)
                else:
                    items.append(response)
            i += 1

    #print ("main done")
    return items, bad_dois

def request_work_list_from_dois(dois_to_request, no_workers=20):
    loop = asyncio.get_event_loop()
    #loop = asyncio.start_event_loop()
    items, bad_dois = loop.run_until_complete(main_request_loop(dois_to_request, no_workers))

    #print(len(items), "/", len(dois_to_request))
    #print("done")
    return items, bad_dois




if __name__ == "__main__":
    
    init_mode = 6
    
    print(os.getcwd())
    print(os.listdir())
    
#     dirname = "crashworthiness"
#     dirname = "ehla"
#     dirname = "engineering"
#     dirname = "structural_optimization"
#     dirname = "deep_learning"
#     dirname = "ea"
    #dirname = "additive_manufacturing"
    #dirname = "driver_glance_behaviour"
    dirname = "cov"
    
    print(dirname in os.listdir())
    
    if not dirname in os.listdir():
        os.mkdir(dirname)
    else:
#         raise DirAlreadyExistsException
        print("Warning: Dir. {} already exists. Continue?".format(dirname))
#         raise

    os.chdir(os.path.join(os.getcwd(), dirname))
    
    if init_mode == 1:
        
        query_list = ["driver glance behaviour"]
        
        #query_list = ["weld", "schweißen", "schweißverfahren", "geschweißt", "welding", "soldering", "brazing", "weldment", "cladding"]
#        query_list = [
#            "additive manufacturing", 
#            "laser cladding", 
#            "electron beam melting", 
#            "selective laser melting", 
#            "selective laser sintering",
#            "rapid prototyping",
#            "rapid tooling",
#            "fused deposition modeling",
#            "laser metal deposition lmd",
#            "laser metal additive manufacturing"
#        ]
#         
#         query_list = [
#             "laser metal deposition lmd",
#             "laser metal additive manufacturing",
#             "laser cladding"
#             ]
        
#         query_list = [
#             "crashworthiness"
#             ]
        
        
#         query_list = [
#             "laser metal deposition",
#             "direct metal deposition",
#             "direct material deposition",
#             "laser cladding"
#             ]

#         query_list = [
#             "Product-Service System",
#             "Hybrides Leistungsbündel",
#             "Life Cycle Planning Engineering",
#             #"Life Cycle Engineering"
#             ]
        
        #query_list = ["engineering"]
        
#         query_list = [
#             "topology optimization",
#             "topography optimization",
#             "shape optimization",
#             "multidisciplinary design optimization",
#             "generative engineering design",
#             "design optimization",
#             "reliability optimization",
#             "reliability based design optimization",
#             "manufacturing constraints optimization",
#             "structural optimization",
#             "crashworthiness",
#             "isogeometric analysis and optimization",
#             "xfem",
#             "simp",
#             "eso beso",
#             "weld welding optimization",
#             "welding simulation",
#             "battery package optimization",
#             "optimization for additive manufacturing",
#             "Evolutionary Structural Optimization",
#             "Automotive Design Optimization",
#             "Aerospace Design Optimization",
#             "Thermal and Fluid Design Optimization",
#             "Composite Multifunctional Materials",
#             "Design Under Uncertainty",
#             "Evolutionary and Heuristic Optimization",
#             "Inverse Problems and Parameter Identification",
#             "Multi-Objective Optimization",
#             "robust optimization",
#             "sensitivity analysis",
#             "size optimization sizing",
#             "smart structures",
#             "CAD FEM integration",
#             "Modelling, Simulation, and Design"
#             ]
        
#         query_list = [
#             "deep learning",
#             "neural network",
#             "natural language processing",
#             "variational autoencoder",
#             "neural turing machine",
#             "reinforcement learning",
#             "machine learning",
#             "machine translation",
#             #"computer vision",
#             "Generative Adversarial Network GAN",
#             "speech synthesis"
#             ]
        
#         query_list = [
#             "multidisciplinary design optimization"
#             ]              
        
#         query_list = [
#             "evolutionary algorithm",
#             "genetic algorithm",
#             "evolutionary computation"
#             ]              
#                       

        wl = []
        for q in query_list:
            #wl += request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2010}, max_results=5000)
            #wl += request_work_list(q, filters={"from-pub-date" : 2008}, max_results=20000)
            wl += request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2008}, max_results=20000)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        pickle.dump(work_dict, open("work_dict_lvl0.p", "wb"))
            
    elif init_mode == 2:
        
        #work_dict = pickle.load(open("work_dict2.p", "rb"))
        work_dict = pickle.load(open("work_dict_lvl0.p", "rb"))
    
    elif init_mode == 3:
        
        filename = "ilt_doi-list_20180427_he.txt"
        
        f = open(filename, "r")
        lines = f.readlines()
        f.close()
        dois = [l.strip() for l in lines]
        
        print(len(dois))
        
        wl, bad_dois = request_work_list_from_dois(dois, 35)
        
        pprint(bad_dois)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        pickle.dump(work_dict, open("work_dict_lvl0.p", "wb"))
        
        #return
    
    elif init_mode == 4:
        
        company = "volkswagen"
        
        q = {"affiliation": company}
        
        wl = request_work_list(q)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        pickle.dump(work_dict, open("work_dict_lvl0.p", "wb"))
    
    elif init_mode == 5:
                
        q = {"author": "Susanne Häußler", "title": "Pseudomonas aeruginosa"}
        q = {"title": "Pseudomonas aeruginosa"}
        #q = ["Pseudomonas aeruginosa"]
        
        
        wl = request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2000}, max_results=50000)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        pickle.dump(work_dict, open("work_dict_lvl0.p", "wb"))
    
    elif init_mode == 6:
        
        doi_lines = []
        with open("cov/input_dois.txt", "r") as f:
            doi_lines = f.read().split("\n")
        
        wl = request_work_list_from_dois(doi_lines)
        
        q = {"title": "covid-19"}
        #q = ["Pseudomonas aeruginosa"]
        
        
        wl = request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2019}, max_results=5000)
        
        

    ref_counts = get_ref_counts(work_dict)
    
    

    for iter in range(3):
        
        print("################\n  Level {}\n################".format(iter))
        
        # update work_dict with referenced works
        
        ref_dois = set()
        
        for w in work_dict.values():
            ref_dois.update(get_reference_dois(w))
        print(len(ref_dois), len(ref_dois - work_dict.keys()), len(work_dict.keys()))
        
        dois_to_request = list(ref_dois - work_dict.keys())
        print(len(dois_to_request))
        
        if iter == 0:
            min_ref_count = 3
        elif iter == 1:
            min_ref_count = 3
        else:
            min_ref_count = 3
        dois_to_request = [doi for doi in dois_to_request if ref_counts[doi] >= min_ref_count]
        print(len(dois_to_request))
        
        time.sleep(1)
        i = 0
        k = 1000
        work_list = []
        while i < len(dois_to_request):
            j = i + k
            new_items, bad_dois = request_work_list_from_dois(dois_to_request[i:j], 30)
            i = j
            
            work_list += new_items
            #work_dict = update_work_dict(work_dict, new_items)
            print(j, "/", len(dois_to_request))
        
        work_dict = update_work_dict(work_dict, work_list)
        ref_counts = get_ref_counts(work_dict)
        
        len(work_dict), len(work_list)
        
        print("bad DOIs:", bad_dois)
    
        pickle.dump(work_dict, open("work_dict_lvl{}.p".format(iter+1), "wb"))
    
    print("bad DOIs:", bad_dois)



    

