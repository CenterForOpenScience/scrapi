from datetime import date, timedelta


def consume():

    today = date.today()
    yesterday = today - timedelta(4)

    month = today.strftime('%m')
    day = today.strftime('%d')
    year = today.strftime('%Y')

    y_month = yesterday.strftime('%m')
    y_day = yesterday.strftime('%d')
    y_year = yesterday.strftime('%Y')

    base_url = 'http://clinicaltrials.gov/ct2/results/download?down_stds=all&' 
    url_middle = 'down_typ=results&down_flds=shown&down_fmt=plain&lup_s=' 

    url_end = '{}%2F{}%2F{}%2F&lup_e={}%2F{}%2F{}&show_down=Y'.format(y_month, y_day, y_year, month, day, year)

    url = base_url + url_middle + url_end

    print url

consume()