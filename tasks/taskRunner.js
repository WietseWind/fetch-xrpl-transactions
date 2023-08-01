class TaskRunner {
  shouldRun = true
  errored = false

  constructor() {
  }

  async start(task) {
    while (this.shouldRun) {
      try {
        await task()
      } catch(e) {
        console.error('Error:', e)
        this.errored = true
        break
      }
    }
  }

  stop() {
    this.shouldRun = false
  }
}

module.exports = {
  TaskRunner,
}
