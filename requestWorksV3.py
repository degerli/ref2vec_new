#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created on Tue Aug 20 11:27:22 2019

@author: FBOCKNI
"""


import asyncio
import requests
import numpy as np
import pandas as pd
from pprint import pprint
from collections import defaultdict
from copy import deepcopy
import pickle
import os
import json

from sqlalchemy import create_engine
#engine = create_engine('sqlite:///crawl_db.sqllite', echo=True)
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, String, Binary, DateTime, Boolean, Date, Text

from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

from sqlalchemy.orm import sessionmaker
#Session = sessionmaker(bind=engine)
Session = sessionmaker()

from datetime import datetime, date


from operator import itemgetter

class CrossRefWork(Base):
    __tablename__ = 'cr_work_data'
    
    id = Column(Integer, primary_key=True)
    doi = Column(String(256))
    doi_prefix = Column(String(64))
    doi_suffix = Column(String(192))
    doi_isverylong = Column(Boolean)
    doi_verylong = Column(Text)
    title = Column(String(512))
    author_names = Column(String(512))
    date_issued = Column(Date)
    container_title = Column(String(512))
    keywords = Column(String(512))
    abstract = Column(Text)
    issn_1 = Column(String(64))
    issn_2 = Column(String(64))
    issn_3 = Column(String(64))
    volume = Column(String(32))
    issue = Column(String(32))
    first_page = Column(String(32))
    last_page = Column(String(32))
    cr_type = Column(String(32))
    
#    work_json_content = Column(Text)
    
    ref_count = Column(Integer)
    cit_count = Column(Integer)
    
    crawl_level = Column(String(8))
    
#    bContent = Column(Text)
    
    json_content = Column(Text)
    
    def __repr__(self):
        if self.date_issued:
            return "<CrossRefWork {} ({}): {}. http://dx.doi.org/{} >".format(self.author_names, self.date_issued.year, self.title, self.doi)
        else:
            return "<CrossRefWork {} ({}): {}. http://dx.doi.org/{} >".format(self.author_names, "XXXX", self.title, self.doi)


class CrossRefReference(Base):
    __tablename__ = 'cr_reference_data'
    
    id = Column(Integer, primary_key=True)
    
    doi_source = Column(String(256))
    doi_target = Column(String(256))



def get_db_entry_from_work(work, level):
    
    doi = work["DOI"]
    
    work_for_db = CrossRefWork(doi=doi, doi_isverylong=False, crawl_level=level)
    if len(doi) > 256:
        work_for_db.doi_isverylong = True
    work_for_db.doi_prefix, work_for_db.ydoi_suffix = doi.split("/", 1)
    
    year = -1
    month = 1
    day = 1
    if "issued" in work:                
        date_tuple = work["issued"]["date-parts"][0]
        if len(date_tuple) >= 1:
            year = date_tuple[0]
        if len(date_tuple) >= 2:
            month = date_tuple[1]
        if len(date_tuple) >= 3:
            day = date_tuple[2]
    if year:
        work_for_db.date_issued = date(year, month, day)                
    
    title = ""
    if "title" in work:
        for t in work["title"]:
            title += t  # + "\n"
            break
    work_for_db.title = title[:512]
    
    author_names = ""
    if  "author" in work:
        author_names = ", ".join([a["family"] for a in work["author"] if "family" in a])
        work_for_db.author_names = author_names
    
    if "abstract" in work:
        work_for_db.abstract = work["abstract"]
    
    if "container-title" in work:
        #container_title = ""
        container_title = "\n".join([ct for ct in work["container-title"]])
        
    if "issn-type" in work:
#                    print(work["issn-type"])
        for i, issn_type in enumerate(work["issn-type"]):
#                        print(i, issn_type)
#            issn_work_counts[issn_type["value"]] += 1
            if i==0:
                work_for_db.issn_1 = issn_type["value"]
            elif i==1:
                work_for_db.issn_2 = issn_type["value"]
            elif i==2:
                work_for_db.issn_3 = issn_type["value"]
    elif "ISSN" in work:
        for i, issn in enumerate(work["ISSN"]):
#            issn_work_counts[issn] += 1
            if i==0:
                work_for_db.issn_1 = issn
            elif i==1:
                work_for_db.issn_2 = issn
            elif i==2:
                work_for_db.issn_3 = issn
    
    if "volume" in work:
        work_for_db.volume = work["volume"]
        
    if "issue" in work:
        work_for_db.issue = work["issue"]
        
    if "page" in work:
        ps = work["page"].split("-")
        if len(ps) >= 1:                        
            work_for_db.first_page = ps[0]
        if len(ps) >= 2:                        
            work_for_db.first_page = ps[-1]
    
    if "type" in work:
        work_for_db.cr_type = work["type"]
    
    if "references-count" in work:
        work_for_db.ref_count = work["references-count"]
    else:
        work_for_db.ref_count = -1
    
    if "is-referenced-by-count" in work:
        work_for_db.cit_count = work["is-referenced-by-count"]
    else:
        work_for_db.cit_count = -1
        
    work_for_db.json_content = json.dumps(work)
#                work_for_db.bContent = json.dumps(work).encode()
    
#    db_session.add(work_for_db)
#    
#    if "reference" in work:
#        for ref in work["reference"]:
#            if "DOI" in ref:
#                ref_for_db = CrossRefReference(doi_source=doi, doi_target=ref["DOI"])
#                db_session.add(ref_for_db)
    
    return work_for_db


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

#    print("requesting", r.url)

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
        

#        print("requesting", r.url)

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
    
    dirname = "COV"
    
    print(dirname in os.listdir())
    
    if not dirname in os.listdir():
        os.mkdir(dirname)
    else:
#         raise DirAlreadyExistsException
        print("Warning: Dir. {} already exists. Continue?".format(dirname))
#         raise

    os.chdir(os.path.join(os.getcwd(), dirname))
    

    db_engine = create_engine('sqlite:///SMO.sqllite', echo=False)

    drop_db = False
    while True:
        c_input = input("Drop DB? [y/n]")
        if c_input.lower().startswith("y"):
            drop_db = True
            break
        elif c_input.lower().startswith("n"):
            drop_db = False
            break
    
    if drop_db:
        Base.metadata.drop_all(db_engine)
    
    Base.metadata.create_all(db_engine)
    
    Session.configure(bind=db_engine)
    db_session = Session()
    
    if False:
        
        print(1)
        work_dict = pickle.load(open("work_dict_lvl{}.p".format(0.5), "rb"))
        print(2)
        
        dois_in_db = set()
        dois_in_db.update(work_dict.keys())
        
        ref_counts = get_ref_counts(work_dict)
        
        issn_work_counts = defaultdict(int)
        
        for doi, work in work_dict.items():
            
            try:
                work_for_db = CrossRefWork(doi=doi, doi_isverylong=False, crawl_level="0.5")
                if len(doi) > 256:
                    work_for_db.doi_isverylong = True
                work_for_db.doi_prefix, work_for_db.ydoi_suffix = doi.split("/", 1)
                
                year = -1
                month = 1
                day = 1
                if "issued" in work:                
                    date_tuple = work["issued"]["date-parts"][0]
                    if len(date_tuple) >= 1:
                        year = date_tuple[0]
                    if len(date_tuple) >= 2:
                        month = date_tuple[1]
                    if len(date_tuple) >= 3:
                        day = date_tuple[2]
                if year:
                    work_for_db.date_issued = date(year, month, day)                
                
                title = ""
                if "title" in work:
                    for t in work["title"]:
                        title += t  # + "\n"
                        break
                work_for_db.title = title[:512]
                
                author_names = ""
                if  "author" in work:
                    author_names = ", ".join([a["family"] for a in work["author"] if "family" in a])
                    work_for_db.author_names = author_names
                
                if "abstract" in work:
                    work_for_db.abstract = work["abstract"]
                
                if "container-title" in work:
                    #container_title = ""
                    container_title = "\n".join([ct for ct in work["container-title"]])
                    
                if "issn-type" in work:
#                    print(work["issn-type"])
                    for i, issn_type in enumerate(work["issn-type"]):
#                        print(i, issn_type)
                        issn_work_counts[issn_type["value"]] += 1
                        if i==0:
                            work_for_db.issn_1 = issn_type["value"]
                        elif i==1:
                            work_for_db.issn_2 = issn_type["value"]
                        elif i==2:
                            work_for_db.issn_3 = issn_type["value"]
                elif "ISSN" in work:
                    for i, issn in enumerate(work["ISSN"]):
                        issn_work_counts[issn] += 1
                        if i==0:
                            work_for_db.issn_1 = issn
                        elif i==1:
                            work_for_db.issn_2 = issn
                        elif i==2:
                            work_for_db.issn_3 = issn
                
                if "volume" in work:
                    work_for_db.volume = work["volume"]
                    
                if "issue" in work:
                    work_for_db.issue = work["issue"]
                    
                if "page" in work:
                    ps = work["page"].split("-")
                    if len(ps) >= 1:                        
                        work_for_db.first_page = ps[0]
                    if len(ps) >= 2:                        
                        work_for_db.first_page = ps[-1]
                
                if "type" in work:
                    work_for_db.cr_type = work["type"]
                
                if "references-count" in work:
                    work_for_db.ref_count = work["references-count"]
                else:
                    work_for_db.ref_count = -1
                
                if "is-referenced-by-count" in work:
                    work_for_db.cit_count = work["is-referenced-by-count"]
                else:
                    work_for_db.cit_count = -1
                    
                work_for_db.json_content = json.dumps(work)
#                work_for_db.bContent = json.dumps(work).encode()
                
                db_session.add(work_for_db)
#                
                if "reference" in work:
                    for ref in work["reference"]:
                        if "DOI" in ref:
                            ref_for_db = CrossRefReference(doi_source=doi, doi_target=ref["DOI"])
                            db_session.add(ref_for_db)
            
            except KeyError as e:
                print("KeyError {} {} {}".format(e, doi, work))
            except Exception as e:
#                print("Exception {} {} {} {}".format(type(e), e, doi, work))
                print("Exception {} {} {} {}".format(type(e), e, doi, ""))
                
            
        db_session.commit()
        
        del work_dict
    
    if True:
        
        dois_in_db = set()
#        dois_in_db_since2008 = set()
        ref_counts = defaultdict(int)
        issn_work_counts = defaultdict(int)
        
        for work_db in db_session.query(CrossRefWork).yield_per(1000):
            
            if not work_db.doi in dois_in_db:
                work = json.loads(work_db.json_content)
                work_refs = get_reference_dois(work)
                
                dois_in_db.add(work_db.doi)
#                if work_db.date_issued:
#                    if work_db.date_issued.year >= 2008:
#                        dois_in_db_since2008.add(work_db.doi)
                
                for ref in work_refs:
                    ref_counts[ref] += 1
                
                if work_db.date_issued:
                    if work_db.date_issued.year >= 2008:
                        if work_db.issn_1:
                            issn_work_counts[work_db.issn_1] += 1
                        if work_db.issn_2:
                            issn_work_counts[work_db.issn_2] += 1
                        if work_db.issn_3:
                            issn_work_counts[work_db.issn_3] += 1
                
    
    min_ref_count = 3
    dois_to_request = ref_counts.keys() - dois_in_db
    dois_to_request = [doi for doi in dois_to_request if ref_counts[doi] >= min_ref_count]
    
    print(sorted(issn_work_counts.items(), key=itemgetter(1)))
    
    issns_requested = set()
    
    for issn, count in sorted(issn_work_counts.items(), key=itemgetter(1), reverse=True):            
            if count >= 5 and count <= 6:
                if not issn in issns_requested:
                    try:
                        journalTitle, publisher, count_total = request_journal_infos(issn, count_since_year=2008)                        
                        print(journalTitle, publisher, count_total, count)
                        
                        issn_was_requested = False
                        
                        if count_total > 0:
                            if count < count_total:
                                if float(count) / float(count_total) >= 0.01:
                            
                                    print("requesting ISSN", issn, "(count: {})".format(count))
                                    #issns_requested.add(issn)
                                    wl = request_work_list_from_issn(issn, filters={"from-pub-date" : 2008})
    #                                    work_list += wl                        
                                    if len(wl) > 0:
                                        issns_requested.add(issn)
                                    #pprint("issns_requested:", issns_requested)
                                    print("issns_requested:", len(issns_requested), "of", len(issn_work_counts.keys()))
                                    #pprint(issns_requested)
                                    issn_was_requested = True
                                    
                                    for work in wl:
                                        doi = work["DOI"]
                                        if not doi in dois_in_db:                                        
                                            work_for_db = get_db_entry_from_work(work, level="2.5")
                                            db_session.add(work_for_db)
                                        dois_in_db.add(doi)
                                    
                                    db_session.commit()
                            else:
                                print("Journal", journalTitle, "already in db", issn, count, count_total)
                                    
                        if not issn_was_requested:
                            print("ISSN", issn, "not requested")
                        
                    except requests.HTTPError as e:
                        print("HTTPError", e)
                        issns_requested.add(issn)
                    except requests.exceptions.SSLError as e:
                        print("SSLError", e) 
                    except requests.exceptions.ProxyError as e:
                        print("ProxyError", e) 
                    finally:
                        print()
    
    
    
    
    

    

