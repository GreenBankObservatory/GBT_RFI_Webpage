import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def broad_plot(rfi):
    values    = pd.read_csv("{}".format(rfi),usecols = ["frequency_mhz", "intensity_jy"])
    valuex    = values["frequency_mhz"]
    valuey    = values["intensity_jy"]

    plt.xlabel("Frequency_Mhz")
    plt.ylabel("Intensity_Jy")
    plt.plot(valuex, valuey)
    plt.show()


def pick_range(rfi,start,stop):
    values    = pd.read_csv("{}".format(rfi), usecols=["frequency_mhz", "intensity_jy"])
    frequency = values["frequency_mhz"]
    intensity = values["intensity_jy"]

    desired_range       = frequency.between(start,stop, inclusive = True)
    values_listx = []
    values_listy = []

    for i in range(len(frequency)):
        if desired_range[i] == True:
            values_listx.append(frequency[i])
            values_listy.append(intensity[i])
        else:
            None


    plt.xlabel("Frequency_Mhz")
    plt.ylabel("Intensity_Jy")
    plt.plot(values_listx, values_listy)
    plt.show()
