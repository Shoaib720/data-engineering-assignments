import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_horizontal_barplot(data, x,y,x_label,y_label,title):

    # Initialize the matplotlib figure
    f, ax = plt.subplots(figsize=(15, 7))

    # Plot the total crashes
    sns.set_color_codes("pastel")
    sns.barplot(x=x, y=y, data=data.toPandas(),
                label=title, color="b",orient='h',width=0.5)

    # Add a legend and informative axis label
    ax.legend(ncol=2, loc="lower right", frameon=True)
    ax.set(ylabel=y_label,
        xlabel=x_label)
    sns.despine(left=True, bottom=True)
    f.savefig(f'plots/{title}.png')
# print(df.show())