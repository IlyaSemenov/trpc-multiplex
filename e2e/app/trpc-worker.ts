import { startMultiplexWorker } from "trpc-multiplex/worker"

import { transport } from "./config"

startMultiplexWorker(transport)
