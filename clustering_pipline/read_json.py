# -*- coding: utf-8 -*-
import pandas, cPickle
data = pandas.read_json('data\\extractedFromElastic\\results_all.json')
data_ = data['hits']['hits']

# data= cPickle.load( open( "result.p", "rb" ) )
# data_ = data['hits']['hits']
print len(data_)
length_arr, fails = [], []

for i, x in enumerate(data_[:100]):
    df = pandas.DataFrame(columns=['id', 'log', 'fail', 'build_rows', 'tokens_length', 'timestamp'])
    suc = x['fields']['data.result']
    timestamp = x['fields']['@timestamp'][0].encode('utf8')
    build_id = x['_id']
    if suc[0] == u'SUCCESS':
        suc = int(0)
    else:
        suc = int(1)
    fails.append(suc)

    log = x['fields']['message']
    length = len(log)
    length_arr.append(length)
    # print suc , length
    for j, row in enumerate(log):
        tokens_counter = len(row.split())
        df.loc[str(i) + str(j)] = pandas.Series({'id': str(build_id)+'_'+str(j), 'log': row.split(), 'fail': suc, 'build_rows':length, 'tokens_length' : tokens_counter, 'timestamp': timestamp})
        print i, '_', j
    print df.shape
    # df.to_pickle('data/builds/timestamp/builds_{0}__{1}__{2}.pkl'.format(build_id, suc, timestamp[:16].encode('utf8')))

    df.to_pickle('data/builds/timestamp/builds_{0}__{1}__{2}.pkl'.format(build_id, suc, timestamp[:10]))
