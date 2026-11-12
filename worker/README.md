# Worker Diagram

On this branch workers batch locally and hand complete bytes to their own
primary. Peer Batch/ACK quorum is no longer on the normal path. See
[GRBC implementation notes](../docs/GRBC.md); the diagram below is historical.

The diagram below illustrates the worker's architecture and could be useful to keep in mind while going through the code.

<p align="center">
  <img src="https://github.com/asonnino/narwhal/blob/master/.assets/diagram-worker.svg">
</p>
