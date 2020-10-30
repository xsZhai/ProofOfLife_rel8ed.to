import requests,  kumihotools, re
from lxml import html
import pandas as pd
from py2neo import Graph
import time
import json
server = kumihotools.checkwriter()
g = Graph(f'bolt://{server}', user='***', password='***')
Kumi = kumihotools.Kumi()

def toDomain(link):
    return (re.sub(r'^(https?://)?(www\d?\.)?','', link))


def sendToRatings(corpid):
        global server
        query = 'match (c:Corp) where id(c)=%s call apoc.path.subgraphAll(c,{maxLevel:1,labelFilter: "CorpName|Person|Address|ID|Metric|Firmographic|ContactInfo|Phone|Website|Email|Rating|OnlineAccount|Event|Alert|Inspection|Category|Designation|Activity|ClusterType|End|Start|MatchResult"}) yield nodes, relationships  with nodes unwind(nodes) as node with  id(node) as id, %s as corp_id, labels(node) as label0,node unwind label0 as label return id, corp_id,label,node' % (corpid,corpid)
        try:
            g = Graph('bolt://%s' % server, user='system', password='Xg6wF3eDKzsK<2=;')
            data = g.run(query).data()
        except Exception as e:
            time.sleep(10)
            server = kumihotools.checkwriter()
            print('change neo server to ',server)
            g = Graph('bolt://%s' % server, user='system', password='Xg6wF3eDKzsK<2=;')
            data = g.run(query).data()
        if data:
            kumihotools.sendQ(json.dumps(data,ensure_ascii=False),'KumihoEyeRating_BBB2')
            kumihotools.sendQ(json.dumps(data,ensure_ascii=False),'KumihoEyeRating_Yelp2')


def parseSocialLink(web,corpid):
    # web="resultspositive.com/"
    fb="//a[contains(@href,'facebook')]/@href"
    twt="//a[contains(@href,'twitter')]/@href"
    ins="//a[contains(@href,'instagram')]/@href"
    lk="//a[contains(@href,'linkedin')]/@href"

    statusCode, webstatus = kumihotools.webstatus(web)
    query = f'match (w:Website) where w.url = "{web}" set w.status = "{webstatus}", w.statusCode = {statusCode}, w.updateTime = timestamp()'

    try:
        res=requests.get('http://'+web, timeout=10)
        #webstatuscheck
        statusCode = res.status_code

        if statusCode in [523, 503, 502, 500, 410, 409, 404]:
            query = f'match (w:Website) where w.url = "{web}" set w.status = "inactive", w.statusCode = {statusCode}, w.updateTime = timestamp()'
            kumihotools.sendQ(query, 'KumihoBiteWebsiteStatus')
        else:
            query = f'match (w:Website) where w.url = "{web}" set w.status = "active", w.statusCode = {statusCode}, w.updateTime = timestamp()'
            kumihotools.sendQ(query, 'KumihoBiteWebsiteStatus')

        data_facebook=html.fromstring(res.content).xpath(fb)
        data_twitter=html.fromstring(res.content).xpath(twt)
        data_instagram=html.fromstring(res.content).xpath(ins)
        data_linkedin=html.fromstring(res.content).xpath(lk)
        
    except Exception as e:
        query = f'match (w:Website) where w.url = "{web}" set w.status = "Presumed Inactive", w.statusCode = "time out", w.updateTime = timestamp()'
        kumihotools.sendQ(query, 'KumihoBiteWebsiteStatus')
        return


    query_facebook,query_instagram,query_linkedin = '','',''
    if data_facebook and 'javascript' not in data_facebook:
        
        facebook=toDomain(data_facebook[0])
        query_facebook="merge (oa:OnlineAccount:ContactInfo {url:'%s'}) on create set oa.source='%s' with oa match (oap:OnlineAccountProvider {name: 'facebook'}) with oap,oa merge (oap)<-[:PROVIDED_BY]-(oa) with oa match (co:Corp) where id(co)=%s merge (co)-[:REACHED_AT]->(oa)" % (facebook,web,corpid)
    if data_twitter and 'javascript' not in data_twitter:
        #sendtoTwittercorpCaller also changed for pol below
        nameandid={}
        twitter=toDomain(data_twitter[0])
        screenname=twitter.split('/')[-1]
        nameandid['corpid']=corpid
        nameandid['keyword']=screenname
        kumihotools.sendQ(json.dumps(nameandid),'KumihoEyeTwitterCorpCaller')

    
    if data_instagram and 'javascript' not in data_instagram:
        instagram=toDomain(data_instagram[0])
        query_instagram="merge (oa:OnlineAccount:ContactInfo {url:'%s'}) on create set oa.source='%s' with oa match (oap:OnlineAccountProvider {name: 'instagram'}) with oap,oa merge (oap)<-[:PROVIDED_BY]-(oa) with oa match (co:Corp) where id(co)=%s merge (co)-[:REACHED_AT]->(oa)" % (instagram,web,corpid)
    if data_linkedin and 'javascript' not in data_linkedin:
        linkedin=toDomain(data_linkedin[0])
        query_linkedin="merge (oa:OnlineAccount:ContactInfo {url:'%s'}) on create set oa.source='%s' with oa match (oap:OnlineAccountProvider {name: 'linkedin'}) with oap,oa merge (oap)<-[:PROVIDED_BY]-(oa) with oa match (co:Corp) where id(co)=%s merge (co)-[:REACHED_AT]->(oa)" % (linkedin,web,corpid)

    return [query_facebook,query_instagram,query_linkedin]


def handle(body):
    corpid= body.decode()
    web=''
    query_web="match (c:Corp)-[:REACHED_AT]->(w:Website) where id(c)=%s return w.url" % corpid
    data_web=g.run(query_web).data()
    sendToRatings(corpid)
    if data_web:
        webs=[w['w.url'] for w in data_web]
    else:
        return

    for web in webs:
        queries = parseSocialLink(web,corpid)
        if not queries:
            
            continue
        for query in queries:
            if query:
                yield query


Worker = kumihotools.KumihoWorker('KumihoEyePol', handle, outtoq='KumihoBiteSocialMedia')
Worker.startq()

