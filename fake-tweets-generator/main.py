import pandas as pd
import json
import random
import asyncio
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

app = FastAPI()

# Load the CSV file into a DataFrame
df_tweets = pd.read_csv("tweets.csv")

# Create a generator function to simulate a stream of data from the DataFrame
async def generate_tweets_stream():
    for _, tweet in df_tweets.iterrows():
        tweet_json = json.dumps(tweet.to_dict()).encode("utf-8")
        yield tweet_json + b"\n\n"
        await asyncio.sleep(random.uniform(1, 5))  # Add a random delay between 1 to 5 seconds

# Create an endpoint to get a continuous stream of tweets
@app.get("/tweet/stream")
async def stream_tweets():
    response = StreamingResponse(generate_tweets_stream(), media_type="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    return response

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
