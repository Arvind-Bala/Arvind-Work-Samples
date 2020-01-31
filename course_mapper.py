
import pandas as pd
import psycopg2 as pg
import pandas.io.sql as psql  
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.svm import LinearSVC
from sklearn.svm import SVC

#Variables that describe the courses dataset
inbound_prediction_filepath = '****'
outbound_filepath = '****'
course_column_name = '****'
inbound_prediction_df = pd.read_csv(inbound_prediction_filepath)[course_column_name].drop_duplicates()

analytics_host_name = '****' 
analytics_db_name = '****'
analytics_db_user = '****'
analytics_db_user_pass = '****'
analytics_connection_string = "host=" + analytics_host_name + " dbname=" + analytics_db_name + " user=" + analytics_db_user + " password=" + analytics_db_user_pass
analytics_connection = pg.connect(analytics_connection_string)
analytics_cursor = analytics_connection.cursor()

related_subs_training_query = """
select distinct
  sub.subject_name,
  related_sub.subject_name as related_subject_name,
  sub_cat.category_name as sub_category_name,
  parent_cat.category_name as parent_category_name
from
  subjects as sub
  INNER JOIN subjects as related_sub
    ON related_sub.subject_id = sub.related_subject_id
    AND related_sub.school_id = '****'
  INNER JOIN categories sub_cat
    ON sub_cat.category_id = related_sub.category_id
    AND sub_cat.parent_category_id IS NOT NULL 
  INNER JOIN categories as parent_cat 
    ON parent_cat.category_id = sub_cat.parent_category_id
where
  sub.related_subject_id IS NOT NULL
  AND related_sub.subject_name != '****'
  AND related_sub.subject_retired IS NULL
"""

subject_train_df = psql.read_sql_query(related_subs_training_query, analytics_connection)

subject_train_df = subject_train_df[['subject_name','related_subject_name']]
subject_train_df['related_subject_idx'] = subject_train_df['related_subject_name'].factorize()[0]
subject_train_df['frequency'] = subject_train_df.groupby(['related_subject_name'])['subject_name'].transform('count')

course_mapped_floor = 5 #The mininimum number of courses that have to be mapped to the related subject to be included in the training dataset
subject_train_df = subject_train_df[subject_train_df.frequency >= course_mapped_floor]

tfidf = TfidfVectorizer(sublinear_tf=True, min_df=5, norm='l2', encoding='latin-1', ngram_range=(1, 2), stop_words='english')
features = tfidf.fit_transform(subject_train_df.subject_name).toarray()
labels = subject_train_df['related_subject_idx']
idx_to_name_dict = subject_train_df[['related_subject_idx','related_subject_name']].drop_duplicates()

idx_to_name_dict.set_index(['related_subject_idx'], inplace=True)

idx_to_name_dict = idx_to_name_dict.to_dict()['related_subject_name']
X_train, X_test, y_train, y_test = train_test_split(features, labels, random_state = 0)
clf = LinearSVC().fit(X_train, y_train)
accuracy_score = clf.score(X_test, y_test)

preds = clf.predict(tfidf.transform(inbound_prediction_df))

outbound_prediction_df = pd.DataFrame(inbound_prediction_df)
outbound_prediction_df['pred_related_subject_idx'] = preds
outbound_prediction_df['pred_related_subject_name'] = outbound_prediction_df.apply(lambda row: idx_to_name_dict[row['pred_related_subject_idx']], axis=1)
outbound_prediction_df.drop(columns=['pred_related_subject_idx'], inplace=True)

outbound_prediction_df.to_csv(outbound_filepath, header=True, index=False)
