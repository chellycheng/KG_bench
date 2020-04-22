import data.kg_dataset as kgd
import stats.statistics as sts
import figs.plot as plt

if __name__ == '__main__':

    # fetch one dataset
    kgd.fetch_fb13()

    # fetch all dataset
    kgd.fetch_all()

    # get all stats dataset
    sts.fetch_statistic_all(output_path='./stats')

    # set stats task and run
    task = ['fb15k,wn18']
    for t in task:
        sts.fetch_statistic(t,output_path='./stats')

    plt.plot_all(output_path='./figs')


