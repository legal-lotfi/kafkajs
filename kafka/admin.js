const { kafka } = require('./index')
const Error = require("http-errors");

class Admin {

    constructor() {
        this.admin = kafka.admin();
        this.admin.connect();
    }

    connect(config = {}) {
        //initialize connection to zookeeper/kafka
        this.admin.connect(config)
        .then(() => {
            console.log("action=kafka_admin_start status=READY")
        })
        .catch(err => {
            console.log("action=kafka_admin_start status=ERROR error=" + err)
        })
    }
}

module.exports = new Admin()