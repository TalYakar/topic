from os import walk
import pandas as pd
import scipy, pickle
import numpy as np


def arr_(df):
    arr = [' '.join(x) for x in df.log.tolist()]
    return arr


def sensitiveCosine(l1, l2):
    l1, l2 = l1.split(), l2.split()
    s = np.sum([1 if x == y else 0 for x, y in zip(l1, l2)])
    return s / float(np.sqrt(len(l1) * (len(l2))))


def adding_features(df, idl):
    """

    :type df: object
    """
    df['logs'] = [' '.join(x) for x in df.log.tolist()]
    df['idl'] = idl
    # df['hash'] = [hash(x) for x in df.logs.tolist()]
    df['cluster'] = -1
    df.drop(df.columns[[1, 3, 4]], axis=1, inplace=True)
    return df

#reading files
thr, ids , logs, df , fail_counter = 0.75, [], [], pd.DataFrame(), 0

# for (dirpath, dirnames, filenames) in walk('builds\\'):
for (dirpath, dirnames, filenames) in walk('data\\builds\\timestamp\\df_11_7_2016'):
    break

for f in filenames:
    idl = str(f.split('__')[0].lstrip('builds_'))
    # ids.append(idl)
    df_ = pd.read_pickle(dirpath +'\\'+ f)
    df_ = adding_features(df_, idl)
    df = pd.concat([df, df_])

df = df.drop_duplicates()
logs = list(set(zip(df.logs, df.id, df.fail)))

# start splitting
n, clusters, i, j, c = len(logs), [], 0, 1, 0
while i < len(logs):# and i <= n:
    try:
        df.ix[df['id'] == logs[i][1], 'cluster'] = i
        cluster_i = [logs[i]]
        j = i + 1
        while j < len(logs) and j <= n:
            dist = sensitiveCosine(logs[i][0], logs[j][0])
            if dist >= thr or (len(logs[i][0]) == 2 and len(logs[j][0]) == 2 and dist >= 0.5):
                logs[j] = logs[j] + (len(clusters),)
                cluster_i.append(logs[j])
                df.ix[df['id'] == logs[j][1], 'cluster'] = i
                logs.remove(logs[j])
                # n -= 1
                j += 1
            else:
                j += 1
        clusters.append(cluster_i)
        # c += 1
        logs.remove(logs[i])
        # n -= 1
        i += 1
        print c, i, len(logs)
        # df_cl.loc[str(i)] = pd.Series(dict(id=ids, cluster=cluster_i, fail=df.fail[0], length=len(cluster_i)))
    except:
        pass

print  '---------' + str(ids)
pickle.dump(clusters, open("results\\time_11_7_16\clusters_builds_.p", "wb"))
df.to_pickle('results\\time_11_7_16\clusters_build_df_all.p')
const = len(filenames)
df.to_pickle('results\\time_11_7_16\clusters_build_df_{0}.p'.format(const))

print '# of clusters' + str(len(clusters))
for n,i in enumerate(clusters):
    print '---cluster-{0}--'.format(str(n))+'\n'
    for i_ in i:
        print i_
        # print '\n'

# print df_.ix[-1]
# pickle.dump( length, open( "length.p", "wb" ) )

# s = pd.Series(length)
# s.to_pickle(dirpath + 'length.pkl')
# print s.describe()
# s = pd.DataFrame(tokenslength)
# s.to_pickle(dirpath+'df_tokenslength_.pkl')
# print s.describe()

# print(scipy.stats.describe(tokenslength))
