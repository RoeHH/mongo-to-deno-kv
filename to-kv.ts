import { MongoClient } from "https://deno.land/x/atlas_sdk@v1.0.2/mod.ts";

import "https://deno.land/x/dotenv@v3.2.0/load.ts";
const secrets = {
  key: Deno.env.get("MONGO_DATA_API_KEY"),
  app: Deno.env.get("MONGO_APP_ID"),
};
if (!secrets.app || !secrets.key) {
  throw new Error("environment variable MONGO_DATA_API_KEY or MONGO_APP_ID not set");
}

const client = new MongoClient({
    endpoint: "https://data.mongodb-api.com/app/" + secrets.app + "/endpoint/data/v1",
    dataSource: "iccee0",
    auth: {
      apiKey: secrets.key,
    },
});

const kv = await Deno.openKv()


export default async function loadDataToMongo(database: string, collection: string, keybase?: string[]) {
    const db = client.database(database);
    const userConfigCollection = db.collection(collection);
    const iter = kv.list<string>({ prefix: keybase !== undefined ? [...keybase] : [database, collection] });
    for await (const res of iter) {
        userConfigCollection.insertOne(JSON.parse(res.value));
    }
}


const autoRunSecrets = {
    db: Deno.env.get("MONGO_TO_KV_DB"),
    col: Deno.env.get("MONGO_TO_KV_COLLECTION"),
    key: Deno.env.get("MONGO_TO_KV_KEY_BASE"),
    updateTime: Deno.env.get("MONGO_TO_DAYS_TILL_RELOAD"),
    fetchMoreThenOnce: Deno.env.get("MONGO_FETCH_MORE_THEN_ONCE")
};

if (autoRunSecrets.db && autoRunSecrets.col) {
    const lastUpdate = await kv.get<Date>(["MONGO_TO_KV_LAST_UPDATE_DATE"])
    if(lastUpdate.value){
        if(autoRunSecrets.fetchMoreThenOnce === "true"){
            if(autoRunSecrets.updateTime){
                if(new Date(Date.now() - Number(autoRunSecrets.updateTime) * 24 * 60 * 60 * 1000) > lastUpdate.value){
                    loadDataToMongo(autoRunSecrets.db, autoRunSecrets.col);
                    await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());                    
                }
            }else{               
                loadDataToMongo(autoRunSecrets.db, autoRunSecrets.col);
                await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());
            }
        }
    }else{       
        loadDataToMongo(autoRunSecrets.db, autoRunSecrets.col);
        await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());
    }
}
