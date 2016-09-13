from failed import f_failed
from template_extract_ import temp_params, template
import pandas, cPickle

def main():
    df = pandas.read_pickle('results/clusters_build_df_all.p')
    assert isinstance(df, pandas.DataFrame)
    failed = f_failed(df)
    cl, df_, df_params = template(failed)
    df_params = temp_params(df_params)
    cPickle.dump(cl, open("results/templates_df_all.p", "wb"))
    df_.to_pickle('results/failed_clusters_templates_{0}.pkl'.format(len(cl)))



if __name__ == '__main__':
    print main()
# to read :

