import { createClient } from 'redis';

const redisClient = createClient();

redisClient.on('error', err => {
    redisClient.end(false);
    console.error('Redis Client Error', err)
})

await redisClient.connect();

await redisClient.ping();

var pid = 0;

(process.argv[2] == 'getErrors') ? getErrorMessages() : init();

async function init() {
    pid = await redisClient.incr("PID");

    await redisClient.set('generator', pid, { NX: true }) ? initGenerator() : subscribe();
}

async function initGenerator() {
    console.log("Generator PID: " + pid);

    setInterval(async () => {
        await redisClient.expire('generator', 1);

        var message = getMessage();

        console.log("publihed message: " + message);
        publish(message);
    }, 500);

    //    if (message >= 1000000) {
    //        clearInterval(generator);
    //        //process.exit();
    //    }
    //}, 0);

//    generator.unref();
}

async function subscribe() {
    setInterval(async () =>{
        const res = await redisClient.set('generator', pid, { NX: true });
        if (!res) {
            await redisClient.sadd('listeners', pid);
            console.log("new listener with PID: " + pid);
        } else {
            await redisClient.srem('listeners', pid);
            clearInterval(listen);
            initGenerator();
        }
    }, 0);
}

async function publish(message) {
    const listeners = await redisClient.smembers('listeners');

    listeners.forEach(async (listener) => {
        eventHandler(message, async (err, message) => {
            if (err) {
                await redisClient.sadd('error_messages', message);
                console.log("listener " + listener + " error message: " + message);
            } else {
                console.log("listener " + listener + " received message: " + message);
            }
        })
    })

    redisClient.del('listeners');
}

function getMessage() {
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

function eventHandler(msg, callback) {
    function onComplete() {
        var error = Math.random() > 0.85;
        callback(error, msg);
    }

    // processing takes time...
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}

function getErrorMessages() {
    const messages = redisClient.smembers('error_messages');

    console.log('error_messages: ' + messages);
    redisClient.del('error_messages');
    redisClient.end(false);
}