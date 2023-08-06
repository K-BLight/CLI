import { Busylight } from '@4lch4/busylight'
import { Kafka } from 'kafkajs'
import { hostname } from 'os'
import { config } from 'dotenv'
import { join, dirname } from 'path'
import { fileURLToPath } from 'url'

const __dirname = dirname(fileURLToPath(import.meta.url))

config({ path: join(__dirname, '..', '.env'), })

const kafka = new Kafka({
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
  sasl: {
    mechanism: 'scram-sha-256',
    username: process.env.KAFKA_USERNAME || '',
    password: process.env.KAFKA_PASSWORD || '',
  },
  ssl: true,
})

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP || `${hostname().toLowerCase()}-consumer` })

async function main() {
  try {
    const blight = new Busylight()

    await consumer.connect()

    await consumer.subscribe({ topic: process.env.KAFKA_TOPIC || hostname(), fromBeginning: true })

    await consumer.run({
      eachMessage: async ({ message }) => {
        const content: { [key: string]: string | undefined } = JSON.parse(message.value?.toString() || '{}')

        switch (content.status?.toLowerCase()) {
          case 'available':
            await blight.on('green')
            break

          case 'busy':
            await blight.on('red')
            break

          case 'focused':
            await blight.on('purple')
            break

          case 'off':
            await blight.off()
            break

          default:
            await blight.on('yellow')
            break
        }

        console.log(`[main]: Message Content: ${JSON.stringify(content, null, 2)}`)
      },
    })
  } catch (error) {
    console.error(`[main]: Error encountered...`, error)
  }
}

main()
  .then(() => {
    console.log(`[main]: Main completed...`)
  })
  .catch(error => {
    console.error(`[main]: Error encountered...`, error)
  })
