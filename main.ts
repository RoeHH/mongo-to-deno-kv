import { MongoClient } from "https://deno.land/x/atlas_sdk@v1.0.2/mod.ts";

import "https://deno.land/x/dotenv@v3.2.0/load.ts";
import { DBRef } from "https://deno.land/x/web_bson@v0.2.4/mod.ts";
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


export default function loadDataToKV(database: string, collection: string, keybase?: string[]) {
    const db = client.database(database);
    const userConfigCollection = db.collection(collection);
    userConfigCollection.find().then(c => 
        c.forEach(async document => await kv.set(keybase !== undefined ? [...keybase, String(document._id)] : [database, collection, String(document._id)], document))
    )
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
                    loadDataToKV(autoRunSecrets.db, autoRunSecrets.col);
                    await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());                    
                }
            }else{               
                loadDataToKV(autoRunSecrets.db, autoRunSecrets.col);
                await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());
            }
        }
    }else{       
        loadDataToKV(autoRunSecrets.db, autoRunSecrets.col);
        await kv.set(["MONGO_TO_KV_LAST_UPDATE_DATE"], Date.now());
    }
}
