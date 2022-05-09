const { SYSVAR_EPOCH_SCHEDULE_PUBKEY } = require('@solana/web3.js')
const { Kafka } = require('kafkajs')
const config = require('./config')

const client = new Kafka({
  brokers: config.kafka.BROKERS,
  clientId: config.kafka.CLIENTID
})

const topic = config.kafka.TOPIC

const producer = client.producer()

const randomMsg = (len) => {
    let text = ""
    let possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    
    for (let i = 0; i < len; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length))
    
    return text
}

const sleep = (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms)
    })
} 

const sendMessage = async (producer, topic, msgCount) => {
  await producer.connect()

  let id = 0

  while (msgCount --) {

    await sleep(500)

    payloads = {
        topic: topic,
        messages: [
            { value: JSON.stringify({
                id: ++id,
                message: randomMsg(10)
            }) }
        ],
        attributes: 1,
        timestamp: Date.now()
    }
    console.log('payloads=', payloads)
    producer.send(payloads)      
  }
}

sendMessage(producer, topic, 100)
