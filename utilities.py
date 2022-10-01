import pandas as pd

save_location = './data/'


def save_frame(frame, sub_location, file_name):
    frame.to_csv(save_location + sub_location+'\\'+file_name+'.csv')
    return
