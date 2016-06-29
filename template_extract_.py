# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pandas as pd
import cPickle, re
import numpy as np
from collections import Counter
params, temp_counter = [], 0

def sensitiveCosine2(l1, l2):
    l1, l2 = l1.split(), l2.split()
    # l1, l2 = re.findall(r"[\w']+", l1), re.findall(r"[\w']+", l2)
    s = np.sum([1 if x == y else 0 for x, y in zip(l1, l2)])
    return s / float(np.sqrt(len(l1) * (len(l2))))


def param(x):
    global params
    global temp_counter
    params.append(x)
    temp_counter+=1
    return '<<' + x + '>>'


def prob(l,j):
    return [float(l_)/j for l_ in l]


# cl = cPickle.load(open( "clusters_2_.p", "rb" ))
df_ = pd.DataFrame(columns=['cluster','template', 'length', 'params'])
# df_params = pd.DataFrame(columns=['cluster','template', 'dict_length', 'params'])
df_params = pd.DataFrame(columns=['template', 'dict_length', 'params'])
cl = cPickle.load(open( "results\clusters_failed_.p", "rb" ))

# cl = cPickle.load(open( "data\clusters\clusters_build_AVP-nBVNePvB1HupEh-a_.p", "rb" ))
batch, batch_, outcluster , templates, temp = [], [], [], [], []
for i, cluster in enumerate(cl[1:]):
    print
    print  '---------------cluster' +str(i)+'_length_' + str(len(cluster))+'------------------------\n'
    batch_ = re.findall(r"[\w']+", cluster.logs[0])
    batch = np.array(cluster.logs[0].split())
    print cluster.logs[0].encode('utf-8')
    params, structure = [], {}
    for j, x_ in enumerate(cluster.logs[1:]):
        print x_.encode('utf-8')
        dist = sensitiveCosine2(cluster.logs[0], x_)
        if dist >= 0.2:
            # temp = []
            temp = x_.split()
            # temp = [x if x == y else '<<'+ x +'>>' for x, y in zip(x_.split(), cluster.logs[0].split())]
            # for x, y in zip(x_.split(), cluster.logs[0].split()):
            #     if x == y:
            #         temp.append(x)
            #     else:
            #         temp.append('<'+x+'>')
            #         # params.append(x)
            #         # temp_counter += 1
            if len(temp) >= 1 and len(temp) == len(cluster.logs[0].split()):
                batch = np.vstack((batch, temp))
                # structure[j] = ' '.join(temp)
            # elif len(temp) == 1:
            #     batch = np.vstack((batch, temp))
            #     # structure[j] = temp
            # # batch_ = batch + re.findall(r"[\w']+", x_)
            else:
            # cluster.remove(x_)
                outcluster.append(x_)
    thr2 = round(0.85 * len(cluster))
    if batch != []:
        template, p = [], 0
        for j, x in enumerate(batch.T):
            w_dict = Counter(x)
            # temp = [k if v >= thr2 else '<p>' for k, v in w_dict.iteritems() ]
            if len(w_dict) > 1:
                params.append(w_dict)
                template.append('<p_'+str(len(w_dict))+'>')
                df_params.loc[str(i) + '_' + str(j)+'_' + str(p)] = pd.Series(
                    dict( template=' '.join(template), dict_length=len(w_dict), params=w_dict))
                p =+ 1

            else:
                template.append(max(w_dict, key=w_dict.get))

            # except:
            #     pass
    print
    print  '---------------template' + str(i) + '_length_' + str(len(template)) + '------------------------\n'
    print (' '.join(template)).encode('utf-8')
    df_.loc[str(i)] = pd.Series(dict(cluster=cluster.logs.ix[0].encode('utf-8'), template=' '.join(template), length=len(cluster), params = params))

cPickle.dump(cl, open("templates.p", "wb"))
cPickle.dump(cl, open("outcluster.p", "wb"))
df_.to_pickle('results/failed_clusters_templates_{0}.pkl'.format(len(cl)))

    # cl[len(cl) + 1] = outcluster
# for i in cl:
#     if len(i) == 0:
#         cl.remove(i)
# df_params.to_pickle('results/failed_clusters_params_{0}.pkl'.format(len(cl)))
    #     batch = batch + x_.split()
    # cluster_dict = Counter(batch)
    # temp_cl = [k for k,v in cluster_dict.iteritems() if v >= len(cluster)-1]

# for i, cluster in enumerate(outcluster):
#     print  '---------------outcluster' +str(i)+'_length_' + str(len(outcluster))+'------------------------\n'
#     for x_ in outcluster:
#         print x_.encode('utf-8')

df_params['sum'] = [sum(df_params.params.ix[i].values()) for i in range(0, len(df_params))]
df_params['prob'] = [df_params.params.ix[i].values() for i in range(0, len(df_params))]

df_params['prob'] =[prob(l,j) for l, j in zip(df_params.prob, df_params['sum'])]
df_params['diversity'] = [list(set(x.values())) for x in df_params.params]
df_params['temp_len'] = [len(x.split()) for x in df_params.template]
df_params.to_pickle('results/failed_clusters_params_{0}.pkl'.format(len(cl)))