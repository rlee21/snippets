# Credit: Although few examples and tutorial inspired and guided this hackathon project
# was mostly based on https://github.com/rsreetech/TextClassificationNNTensorflow

import os; os.environ['TF_XLA_FLAGS'] = '--tf_xla_enable_xla_devices'
import re
import string
import pandas as pd
import numpy as np
import tensorflow as tf
import tensorflow_hub as hub

from tensorflow.keras import layers
from tensorflow.keras import losses
from tensorflow.keras import regularizers

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split

from collections import Counter

print("Tensorflow Version:{}".format(tf.__version__))

print("Num GPUs Available: ", len(tf.config.experimental.list_physical_devices('GPU')))

if tf.test.gpu_device_name():
    print("Default GPU Device:{}".format(tf.test.gpu_device_name()))
else:
    print("Please get a better computer that has a usable GPU")

def remove_emoji(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def remove_url(text):
    url_pattern  = re.compile("http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+")
    return url_pattern.sub(r'', text)
 # converting return value from list to string


def clean_text(text):
    delete_dict = {sp_character: '' for sp_character in string.punctuation}
    delete_dict[' '] = ' '
    table = str.maketrans(delete_dict)
    text1 = text.translate(table)
    #print("cleaned:"+text1)
    textArr= text1.split()
    text2 = ' '.join([w for w in textArr if ( not w.isdigit() and  ( not w.isdigit() and len(w)>3))])

    return text2.lower()

def training():
  train_data= pd.read_csv("data/training_data.psv", sep="|")
  train_data.dropna(axis=0, how="any", inplace=True)
  train_data["Num_words_text"] = train_data["review_body"].apply(lambda x:len(str(x).split()))
  mask = train_data["Num_words_text"] > 2
  train_data = train_data[mask]
  print("===========Train Data =========")
  print(train_data["reason"].value_counts())
  print(len(train_data))
  print("==============================")

  train_data["review_body"] = train_data["review_body"].apply(remove_emoji)
  train_data["review_body"] = train_data["review_body"].apply(remove_url)
  train_data["review_body"] = train_data["review_body"].apply(clean_text)


  test_data= pd.read_csv("data/test_data.psv", sep="|")
  test_data.dropna(axis=0, how ="any", inplace=True)
  test_data["Num_words_text"] = test_data["review_body"].apply(lambda x:len(str(x).split()))
  mask = test_data["Num_words_text"] >2
  test_data = test_data[mask]
  print("===========Test Data =========")
  print(test_data["reason"].value_counts())
  print(len(test_data))
  print("==============================")

  test_data["review_body"] = test_data["review_body"].apply(remove_emoji)
  test_data["review_body"] = test_data["review_body"].apply(remove_url)
  test_data["review_body"] = test_data["review_body"].apply(clean_text)

  X_train, X_valid, y_train, y_valid = train_test_split(train_data["review_body"].tolist(),
    train_data["reason"].tolist(), test_size=0.20, stratify = train_data["reason"].tolist(),
    random_state=0)

  print('Train data len:' + str(len(X_train)))
  print('Class distribution: ' + str(Counter(y_train)))
  print('Valid data len:' + str(len(X_valid)))
  print('Class distribution: ' + str(Counter(y_valid)))

  x_train = np.asarray(X_train)
  x_valid = np.array(X_valid)
  x_test = np.asarray(test_data["review_body"].tolist())

  le = LabelEncoder()

  train_labels = le.fit_transform(y_train)
  train_labels = np.asarray(tf.keras.utils.to_categorical(train_labels))

  valid_labels = le.transform(y_valid)
  valid_labels = np.asarray(tf.keras.utils.to_categorical(valid_labels))

  test_labels = le.transform(test_data["reason"].tolist())
  test_labels = np.asarray(tf.keras.utils.to_categorical(test_labels))

  categories = le.classes_
  print('---- **** Categories ***** ---')
  print(categories)

  train_ds = tf.data.Dataset.from_tensor_slices((x_train, train_labels))
  valid_ds = tf.data.Dataset.from_tensor_slices((x_valid, valid_labels))
  test_ds = tf.data.Dataset.from_tensor_slices((x_test, test_labels))

  # import pdb; pdb.set_trace()
  # print(y_train[:10])
  # train_labels = le.fit_transform(y_train)
  # print('Text to number')
  # print(train_labels[:10])
  # train_labels = np.asarray( tf.keras.utils.to_categorical(train_labels))
  # print('Number to category')
  # print(train_labels[:10])

  #embedding = "https://tfhub.dev/google/tf2-preview/nnlm-en-dim50/1"
  embedding = "https://tfhub.dev/google/tf2-preview/gnews-swivel-20dim-with-oov/1"
  hub_layer = hub.KerasLayer(embedding, input_shape=[], dtype=tf.string, trainable=True)

  # print(x_train[:1])
  # print(hub_layer(x_train[:1]))

  model = tf.keras.Sequential()
  model.add(hub_layer)
  model.add(tf.keras.layers.Dropout(0.5))
  model.add(tf.keras.layers.Dense(48, activation="relu", kernel_regularizer=regularizers.l2(0.05)))
  model.add(tf.keras.layers.Dropout(0.5))
  model.add(tf.keras.layers.Dense(8, activation="softmax", kernel_regularizer=regularizers.l2(0.05)))

  model.summary()
  model.compile(loss=tf.keras.losses.CategoricalCrossentropy(from_logits=True), optimizer='adam', metrics=["CategoricalAccuracy"])

  # Fit the model using the train and test datasets.
  history = model.fit(train_ds.shuffle(2000).batch(128), epochs=30, validation_data=valid_ds.batch(128), verbose=1)

  model.save("compiled_model_20210404_2")

  # Evaluate the model on the test data using `evaluate`
  print("Evaluate on test data")
  results = model.evaluate(x_test, test_labels)
  print("test loss, test accuracy:", results)


  # Generate predictions (probabilities -- the output of the last layer)
  # on test  data using `predict`
  print("Generate predictions for all samples")
  predictions = model.predict(x_test)
  print(predictions)
  predict_results = predictions.argmax(axis=1)
  print(predict_results)


#training()

def approve_or_not(text):
    input_array = np.array([text])
    result = model.predict(input_array)
    categories = np.array(['Approved', 'Astroturfing', 'Big Accusations', 'Client?', 'Not a Client',
        'Other - Ask Customer Care', 'Peer Endorsement', 'Personal Attack'])
    better_result = np.array(list(zip(categories,result[0])))
    return better_result

model = tf.keras.models.load_model("compiled_model_20210404")

for i in range(10):
  input_text = input("type review body: ")
  res = approve_or_not(input_text)
  print("\n\n***************************")
  print("Recommendation: ")
  if float(res[0][1]) > 0.8:
    print("APPROVE")
  else:
    print("DENY")
  print("***************************\n\n")

  print("Details: ")
  print(res)
  print("===========================\n\n")
