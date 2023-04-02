import advertools as adv
import ssl
import time
import pandas as pd
ssl._create_default_https_context = ssl._create_unverified_context

while True:
    nj_1 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml', max_workers=8)
    nj_2 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=100', max_workers=8)
    nj_3 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=200', max_workers=8)
    nj_4 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=300', max_workers=8)
    nj_5 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=400', max_workers=8)
    nj_6 = adv.sitemap_to_df('https://www.nj.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=500', max_workers=8)
    cleveland_1 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml', max_workers=8)
    cleveland_2 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=100', max_workers=8)
    cleveland_3 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=300', max_workers=8)
    cleveland_4 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=200', max_workers=8)
    cleveland_5 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=400', max_workers=8)
    cleveland_6 = adv.sitemap_to_df('https://www.cleveland.com/arc/outboundfeeds/news-sitemap/?outputType=xml&from=600', max_workers=8)
    nypost = adv.sitemap_to_df('https://nypost.com/news-sitemap.xml', max_workers=8)

    #combining in one variable
    all = [nj_1, nj_2, nj_3, nj_4, nj_5, nj_6, cleveland_1, cleveland_2, cleveland_3, cleveland_4, cleveland_5, cleveland_6, nypost]

    result = pd.concat(
        all,
        axis=0,
        join="outer",
        ignore_index=False,
        keys=None,
        levels=None,
        names=None,
        verify_integrity=False,
        copy=True,
    )
    # Drop rows with duplicate values in all columns
    result.drop_duplicates(subset=['loc'], keep='first', inplace=True)

    result.to_csv('sitemap_data.csv', mode='a', index=True, header=not bool(result.shape[0])) # header=False if file already exists, True otherwise
    # Sleep for the specified duration
    time.sleep(43200)
