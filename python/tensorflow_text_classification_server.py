from flask import Flask, request, jsonify
from tensorflow import keras
import numpy as np


app = Flask(__name__)

@app.route("/classify")
def classify():
    text = request.args.get("text", None)
    return approve_or_not(text)

@app.route("/status")
def status():
    return "OK"

def approve_or_not(text):
    payload = {}
    if text:
        model = keras.models.load_model("compiled_model_20210404")
        prediction = model.predict(np.array([text]))
        categories = np.array(["Approved",
                               "Astroturfing",
                               "Big Accusations",
                               "Client?", "Not a Client",
                               "Other - Ask Customer Care",
                               "Peer Endorsement",
                               "Personal Attack"])

        results = np.array(list(zip(categories, prediction[0])))
        recommendation = "APPROVE" if float(results[0][1]) > 0.8 else "DENY"

        for result in results:
            payload[result[0]] = float(result[1]) * 100

        recommendation_payload =  { "Recommendation": recommendation }

        return jsonify( { **recommendation_payload, **payload } )
    else:
        return jsonify( { "Recommendation": "DENY" })


if __name__ == "__main__":
    app.run(host="0.0.0.0")

