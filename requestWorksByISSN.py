'''
Created on 05.09.2018

@author: fbockni
'''


from requestWorksV2 import *

from operator import itemgetter


import nest_asyncio
nest_asyncio.apply()


if __name__ == "__main__":
    
    issns_requested = set()
    
    ########## SMO ################################
    
#     start_issn = "1615-1488"
#     #start_issn = "1615-147X"
#     
#     work_list = []
#     
#     work_list = request_work_list_from_issn(start_issn, filters={"from-pub-date" : 1998})
#     issns_requested.add(start_issn)
#     pprint(issns_requested)
    
    
    #dirname = "anja3"
#    dirname = "web_scraping"
#    dirname = "driver_glance_behaviour"
    #dirname = "SMO"
    dirname = "cov"
    
    
    print(dirname in os.listdir())
    
    if not dirname in os.listdir():
        os.mkdir(dirname)
    else:
#         raise DirAlreadyExistsException
        print("Warning: Dir. {} already exists. Continue?".format(dirname))
#         raise

    os.chdir(os.path.join(os.getcwd(), dirname))
    
    
    if False:
        doi_lines = []
        with open("input_dois.txt", "r") as f:
            doi_lines = f.read().split("\n")
        
        wl, bad_dois = request_work_list_from_dois(doi_lines)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        query_list = [
                "covid-19"
                ]
        
        for q in query_list:
            wl += request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2019}, max_results=10000)
        
        
        work_dict = update_work_dict(work_dict, wl)
        
        pickle.dump(work_dict, open("work_dict_lvl{}.p".format(0), "wb"))
        
    if False:
#         query_list = [
#             "product-service system",
#             "ecological sustainability",
#             "resource efficiency",
#             "circular economy",
#             "product development methods",
#             "strategies for resource efficiency",
#             "business strategy",
#             "sustainability",
#             "future viability",
#             "unique selling point",
#             "product change",
#             "product portfolio",
#             "eco-innovation",
#             "sustainable product design",
#             ]
#         
#         issn_query_list = [
#             "2071-1050",
#             ] 
    
#        query_list = [
#            "web scraping",
#            ]
        
#        query_list = ["driver glance behaviour"]
#        
#        issn_query_list = ["1369-8478"]        
        
        
        query_list = ["surrogate model",
                      "direct numerical simulation",
                      "uncertainty quantification",
                      "gaussian process",
                      "computational intelligence",
                      "multidisciplinary design optimization",
                      "adjoint optimization",
                      "Reduced Order Model fluid dynamics",
                      "polynomial chaos",
                      "shape optimization",
                      "topology optimization",
                      "galerkin",
                      "conditional expectation",
                      "maximum entropy",
                      "Kalman filter",
                      "gpu fem cfd",
                      "Kullback Leibler divergence",
                      "bayesian optimization",
                      "fluid structure interaction",
                      "additive manufacturing",
                      "crashworthiness",
                      ]
        
        issn_query_list = ["1615-1488"]
        
        wl = []
        for q in query_list:
            wl += request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2014}, max_results=1000)
            #wl += request_work_list(q, filters={"from-pub-date" : 2008}, max_results=20000)
#            wl += request_work_list(q, filters={"has-references": 1, "from-pub-date" : 2008}, max_results=200)
        
        for issn in issn_query_list:
            wl += request_work_list_from_issn(issn, filters={"from-pub-date" : 2008})
            issns_requested.add(issn)
        
    
    
        print("issns_requested:")
        pprint(issns_requested)
        
        work_dict = {}
        work_dict = update_work_dict(work_dict, wl)
        
        
        pickle.dump(work_dict, open("work_dict_lvl{}.p".format(0), "wb"))
    
    else:
        work_dict = pickle.load(open("work_dict_lvl{}.p".format(0), "rb"))
    
    for iter in range(0,5):
        
        ref_counts = get_ref_counts(work_dict)
        
        ref_dois = set()
    
        for w in work_dict.values():
            ref_dois.update(get_reference_dois(w))
        print(len(ref_dois), len(ref_dois - work_dict.keys()), len(work_dict.keys()))
        
        dois_to_request = list(ref_dois - work_dict.keys())
        print(len(dois_to_request))
        
        if iter == 0:
            min_ref_count = 1
        else:
            min_ref_count = 3
        dois_to_request = [doi for doi in dois_to_request if ref_counts[doi] >= min_ref_count]
        
        print(len(dois_to_request))
        
        
        if iter >= 0:
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
            pickle.dump(work_dict, open("work_dict_lvl{}.p".format(iter+0.5), "wb"))
            ref_counts = get_ref_counts(work_dict)
            
            #print(len(work_dict), len(work_list))
            
            print("bad DOIs:", bad_dois)
        
        issn_ref_count = defaultdict(int)
        for doi, count in ref_counts.items():
            try:
                work = work_dict[doi]
                for issn in work["ISSN"]:
                    issn_ref_count[issn] += 1
            except KeyError:
                continue
        
        
        issn_tuples = sorted(issn_ref_count.items(), key=itemgetter(1), reverse=True)   
        
        print("issn_ref_count:")
        pprint(issn_tuples)
        
        work_list = []
        
        for issn, count in issn_tuples:
            
            if count >= 10:
                if not issn in issns_requested:
                    try:
                        journalTitle, publisher, count_total = request_journal_infos(issn, count_since_year=2008)                        
                        print(journalTitle, publisher, count_total, count)
                        
                        issn_was_requested = False
                        
                        if count_total > 0:
                            if count < count_total:
                                if float(count) / float(count_total) >= 0.05:
                            
                                    print("requesting ISSN", issn, "(count: {})".format(count))
                                    #issns_requested.add(issn)
                                    wl = request_work_list_from_issn(issn, filters={"from-pub-date" : 2008})
                                    work_list += wl                        
                                    if len(wl) > 0:
                                        issns_requested.add(issn)
                                    #pprint("issns_requested:", issns_requested)
                                    print("issns_requested:")
                                    pprint(issns_requested)
                                    issn_was_requested = True
                                    
                        if not issn_was_requested:
                            print("ISSN", issn, "not requested")
                        
                    except requests.HTTPError as e:
                        print("HTTPError", e)
                        issns_requested.add(issn)
                    except requests.exceptions.SSLError as e:
                        print("SSLError", e) 
                    except requests.exceptions.ProxyError as e:
                        print("ProxyError", e) 
                
        work_dict = update_work_dict(work_dict, work_list)
    
        pickle.dump(work_dict, open("work_dict_lvl{}.p".format(iter+1), "wb"))
