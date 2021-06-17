#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pickle
import shutil
import pandas as pd
from tempfile import TemporaryDirectory
from numpy.testing import assert_equal, assert_almost_equal, assert_raises
import pandas as pd
import os
import matplotlib.pyplot as plt


class MovieDBError(ValueError):
    pass


class MovieDB():
    data_dir = ''

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.numberofmovies = 0
        self.director_id = 0
        moviedf = pd.DataFrame()
        self.movie_file_name = os.path.join(self.data_dir, "movies.csv")
        self.director_file_name = os.path.join(self.data_dir, "directors.csv")

        moviedf.to_csv(self.movie_file_name)
        directordf = pd.DataFrame()
        directordf.to_csv(self.director_file_name)
        return

    def delete_movie(self, movie_id):

        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])
        olddf = olddf.set_index('movie_id', drop=True)
        if movie_id not in olddf.index:
            raise MovieDBError
        newdf = olddf.drop(movie_id, axis=0)
        newdf['director_id'] = newdf['director_id'].astype(int)
        newdf.to_csv(self.movie_file_name)
        return

    def add_movie(self, title, year, genre, dirName):

        # read movies.csv
        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])

        olddf['year'] = olddf['year'].astype(int)
        olddf['movie_id'] = olddf['movie_id'].astype(int)

        # read directors.csv

        olddirectordf = pd.read_csv(self.director_file_name)
        olddirectordf = pd.DataFrame(olddirectordf, columns=['director_id',
                                                             'given_name',
                                                             'last_name'])
        olddirectordf['director_id'] = olddirectordf['director_id'
                                                     ].astype(int)
        split_name = dirName.split()

        if (len(split_name) == 2):
            last_name, given_name = dirName.split()
            last_name = last_name.replace(',', '')
            given_name = given_name.replace(',', '')
        else:
            last_name = split_name[0].replace(',', '')
            given_name = split_name[1:]

        olddf = pd.merge(olddf, olddirectordf, on='director_id', how='outer')
        olddf['dirName'] = (olddf['last_name'] + ',') + olddf['given_name']
        #  print (olddf)

        #  arrange data

        olddf['title'] = olddf['title'].astype(str).str.strip()
        olddf['genre'] = olddf['genre'].astype(str).str.strip()
        olddf['dirName'] = olddf['dirName'].astype(str).str.strip()

        #  create directors

        olddirector = False
        for i in range(0, len(olddirectordf)):

            if (olddirectordf.iloc[i]['given_name'].lower() ==
                    given_name.lower().strip() and
                olddirectordf.iloc[i]['last_name'].lower() ==
                    last_name.lower().strip()):
                olddirector = True
                break
        if not olddirector:
            self.director_id += 1

            directordata = [self.director_id, given_name, last_name]
            newdirectordf = pd.DataFrame([directordata],
                                         columns=['director_id',
                                                  'given_name', 'last_name'])
            newdirectordf = olddirectordf.append(newdirectordf)
            newdirectordf = newdirectordf.set_index('director_id')
            newdirectordf.to_csv(self.director_file_name)

        error = False

        for i in range(0, len(olddf)):
            if (olddf.iloc[i]['title'].lower().strip() ==
                title.lower().strip() and
                olddf.iloc[i]['year'] == year and
                olddf.iloc[i]['genre'].lower().strip() ==
                genre.lower().strip() and
                olddf.iloc[i]['last_name'].lower().strip() ==
                last_name.lower().strip() and
                olddf.iloc[i]['given_name'].lower().strip() ==
                    given_name.lower().strip()):

                error = True
                break
                print('Warning: movie', title,
                      'is already in the database. Skipping...\n')
        if error:
            raise MovieDBError

        if (not error):
            self.numberofmovies += 1
            data = [self.numberofmovies, title, year, genre, self.director_id]
            newdf = pd.DataFrame([data], columns=['movie_id', 'title',
                                                  'year', 'genre',
                                                  'director_id'])
            newdf = olddf.append(newdf)
            newdf = newdf.set_index('movie_id')
            newdf = newdf.iloc[:, :4]
            newdf.to_csv(self.movie_file_name)
            olddf['year'] = olddf['year'].astype(int)
            error = False

            return(self.numberofmovies)

    def add_movies(self, movie_list):
        newdf = pd.DataFrame(movie_list, columns=['title', 'year', 'genre',
                                                  'director'])

        for i in range(0, len(newdf)):
            olddf2 = pd.read_csv(self.movie_file_name)
            olddf2 = pd.DataFrame(olddf2, columns=['movie_id', 'title',
                                                   'year', 'genre',
                                                   'director_id'])
            olddf2['year'] = olddf2['year'].astype(int)
            olddf2['movie_id'] = olddf2['movie_id'].astype(int)

            olddirectordf2 = pd.read_csv(self.director_file_name)
            olddirectordf2 = pd.DataFrame(olddirectordf2,
                                          columns=['director_id',
                                                   'given_name', 'last_name'])
            olddirectordf2['director_id'] =                 olddirectordf2['director_id'].astype(int)

            olddf2 = pd.merge(olddf2, olddirectordf2, on='director_id',
                              how='outer')
            olddf2['dirName'] = (olddf2['last_name'] + ',') +                 olddf2['given_name']

            olddf2['title'] = olddf2['title'].astype(str).str.strip()
            olddf2['genre'] = olddf2['genre'].astype(str).str.strip()
            olddf2['dirName'] = olddf2['dirName'].astype(str).str.strip()

            if newdf.iloc[i, 0] == '' or newdf.iloc[i, 1] == '' or                     newdf.iloc[i, 2] == '' or newdf.iloc[i, 3] == '':
                print ('Warning: movie index', i,
                       'has invalid or incomplete information. Skipping...')
                continue
            elif pd.isnull(newdf.iloc[i, 0]
                           ) or pd.isnull(newdf.iloc[i, 1]) or \
                    pd.isnull(newdf.iloc[i, 2]) or \
                    pd.isnull(newdf.iloc[i, 3]):
                print ('Warning: movie index', i,
                       'has invalid or incomplete information. Skipping...')
                continue
            else:
                error2 = False
                for j in range(0, len(olddf2)):

                    split_name = newdf.loc[i, 'director'].split()

                    if (len(split_name) == 2):
                        last_name, given_name = newdf.loc[i, 'director'
                                                          ].split()
                        last_name = last_name.replace(',', '')
                        given_name = given_name.replace(',', '')
                    else:
                        last_name = split_name[0].replace(',', '')
                        given_name = split_name[1:]

                    if (olddf2.iloc[j]['title'].lower().strip() ==
                        newdf.iloc[i]['title'].lower().strip() and
                            olddf2.iloc[j]['year'] ==
                        newdf.iloc[i]['year'] and
                            olddf2.iloc[j]['genre'].lower().strip() ==
                            newdf.iloc[i]['genre'].lower().strip() and
                        olddf2.iloc[j]['last_name'].lower().strip() ==
                            last_name.lower().strip() and
                        olddf2.iloc[j]['given_name'].lower().strip() ==
                            given_name.lower().strip()):
                        error2 = True
                        print('Warning: movie', olddf2.iloc[j]['title'],
                              'is already in the database. Skipping...')
                        continue

                if not error2:
                    self.add_movie(newdf.iloc[i, 0],
                                   newdf.iloc[i, 1].astype(int),
                                   newdf.iloc[i, 2], newdf.iloc[i, 3])

    def search_movies(self, title=None, year=None, genre=None,
                      director_id=None):
        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])
        if title is None and year is None and genre is None and                 director_id is None:
            raise MovieDBError
        if title == '' and year == '' and genre == '' and director_id == '':
            raise MovieDBError
        elif pd.isnull(title) and pd.isnull(year) and pd.isnull(genre) and                 pd.isnull(director_id):
            raise MovieDBError
        if not olddf['title'].empty:
            olddf['title'] = olddf['title'].str.lower()
        if not olddf['genre'].empty:
            olddf['genre'] = olddf['genre'].str.lower()

        searchstr = ''
        if title != '' and not pd.isnull(title):
            title = title.lower()
            olddf = olddf[olddf['title'] == title]
        if year != '' and not pd.isnull(year):
            olddf = olddf[olddf['year'] == year]
        if genre != '' and not pd.isnull(genre):
            genre = genre.lower()
            olddf = olddf[olddf['genre'] == genre]
        if director_id != '' and not pd.isnull(director_id):
            olddf = olddf[olddf['director_id'] == director_id]

        #         print(olddf)
        movie_list = []
        for i, rows in olddf.iterrows():
            movie_list.append(rows.movie_id)
        return (movie_list)

    def export_data(self):

        # read movies.csv
        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])
        olddf['year'] = olddf['year'].astype(int)
        olddf['movie_id'] = olddf['movie_id'].astype(int).sort_values()
        # read directors.csv

        olddirectordf = pd.read_csv(self.director_file_name)
        olddirectordf = pd.DataFrame(olddirectordf, columns=['director_id',
                                                             'given_name',
                                                             'last_name'])
        olddirectordf['director_id'] =             olddirectordf['director_id'].astype(int)

        olddf = pd.merge(olddf, olddirectordf, on='director_id', how='outer')
        olddf['dirName'] = (olddf['last_name'] + ',') + olddf['given_name']

        #  arrange data
        olddf['title'] = olddf['title'].astype(str).str.strip()
        olddf['genre'] = olddf['genre'].astype(str).str.strip()
        olddf = olddf.drop(['director_id'], axis=1)
        olddf = olddf.drop(['dirName'], axis=1)
        olddf = olddf.set_index(['movie_id'])
        olddf = olddf.sort_values('movie_id', ascending=True)

        cols = list(olddf.columns)
        cols = cols[0:3] + cols[4:5] + cols[3:4]
        olddf = olddf[cols]
        olddf = olddf.rename(columns={'last_name': 'director_last_name',
                                      'given_name': 'director_given_name'},
                             inplace=False)

        return (olddf)

    def token_freq(self):
        olddf = pd.read_csv(self.movie_file_name, encoding='utf-8')
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])
        olddf = olddf['title']

        movies_list = olddf.values.tolist()

        flat_list = ''
        for item in movies_list:
            flat_list = flat_list + ' ' + item.lower()

        corpus_lower = flat_list.split()
        wordfreq = [corpus_lower.count(p) for p in corpus_lower]
        result = dict(list(zip(corpus_lower, wordfreq)))
        return result

    def generate_statistics(self, stat):
        if stat != 'all' and stat != 'movie' and stat != 'genre' and                 stat != 'director':
            raise MovieDBError
        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title',
                                             'year', 'genre', 'director_id'])

        olddf['year'] = olddf['year']
        olddf['movie_id'] = olddf['movie_id'].astype(int)

        olddirectordf = pd.read_csv(self.director_file_name)
        olddirectordf = pd.DataFrame(olddirectordf, columns=['director_id',
                                                             'given_name',
                                                             'last_name'])
        olddirectordf['director_id'] = olddirectordf['director_id'
                                                     ].astype(int)
        #         split_name = dirName.split()
        split_name = ''

        olddf = pd.merge(olddf, olddirectordf, on='director_id', how='outer')
        olddf['dirName'] = (olddf['last_name']) + ', ' + olddf['given_name']

        olddf['title'] = olddf['title'].astype(str).str.strip()
        olddf['genre'] = olddf['genre'].astype(str).str.strip()

        # movie
        movieresult = dict(olddf.groupby('year')['title'].count())

        # genre
        outputdf = olddf.groupby(['genre', 'year']).agg({'year': ['count']})
        result = outputdf.reset_index()
        result = result.set_index('genre')

        genreresult = {}
        for ind, row in result.iterrows():
            genreresult[ind] = {row[0]: row[1]}

        # director
        doutputdf = olddf.groupby(['dirName', 'year']).            agg({'year': ['count']})
        dresult = doutputdf.reset_index()
        dresult = dresult.set_index('dirName')

        dirresult = {}
        for ind, row in dresult.iterrows():
            dirresult[ind] = {row[0]: row[1]}

        #       for movie
        if stat == 'movie':
            return movieresult

        #       for genre
        if stat == 'genre':
            return (genreresult)

        if stat == 'director':
            return (dirresult)

        if stat == 'all':
            allresult = {}
            allresult['movie'] = movieresult
            allresult['genre'] = genreresult
            allresult['director'] = dirresult

            return allresult

    def plot_statistics(self, stat):
        if stat != 'all' and stat != 'movie' and stat != 'genre' and                 stat != 'director':
            raise MovieDBError
        olddf = pd.read_csv(self.movie_file_name)
        olddf = pd.DataFrame(olddf, columns=['movie_id', 'title', 'year',
                                             'genre', 'director_id'])
        olddf['year'] = olddf['year']
        olddf['movie_id'] = olddf['movie_id'].astype(int)

        olddirectordf = pd.read_csv(self.director_file_name)
        olddirectordf = pd.DataFrame(olddirectordf, columns=['director_id',
                                                             'given_name',
                                                             'last_name'])
        olddirectordf['director_id'] = olddirectordf['director_id'
                                                     ].astype(int)
        #         split_name = dirName.split()
        split_name = ''

        olddf = pd.merge(olddf, olddirectordf, on='director_id', how='outer')
        olddf['dirName'] = (olddf['last_name']) + ', ' + olddf['given_name']

        olddf['title'] = olddf['title'].astype(str).str.strip()
        olddf['genre'] = olddf['genre'].astype(str).str.strip()

        # movie
        movieresult = dict(olddf.groupby('year')['title'].count())

        # genre
        outputdf = olddf.groupby(['genre', 'year']).agg({'year': ['count']})
        result = outputdf.reset_index()
        result = result.set_index('genre')

        genreresult = {}
        for ind, row in result.iterrows():
            genreresult[ind] = {row[0]: row[1]}

        # director
        doutputdf = olddf.groupby(['dirName', 'year']
                                  ).agg({'year': ['count']})
        dresult = doutputdf.reset_index()
        dresult = dresult.set_index('dirName')

        dirresult = {}
        for ind, row in dresult.iterrows():
            dirresult[ind] = {row[0]: row[1]}

        #       for movie
        if stat == 'movie':
            D = movieresult

            plt.bar(range(len(D)), list(D.values()), align='center')
            plt.xticks(range(len(D)), list(D.keys()))

            plt.show()
            ax_list = fig.axes
            return ax_list

        #       for genre
        if stat == 'genre':
            return (genreresult)

        if stat == 'director':
            return (dirresult)

        if stat == 'all':
            allresult = {}
            allresult['movie'] = movieresult
            allresult['genre'] = genreresult
            allresult['director'] = dirresult

            return allresult

