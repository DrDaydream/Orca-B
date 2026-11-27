# Primary Diagram
The diagram shows Narwhal's worker digest and payload synchronization paths. Orca-B sends a signed header containing batch references through GRBC. The proposer drains all currently queued worker digests into each eligible header. Before a remote header can trigger ECHO, all referenced batches must be available on local workers. HeaderWaiter resumes suspended headers after PayloadReceiver records the missing receipts.

<p align="center">
  <img src="https://github.com/asonnino/narwhal/blob/master/.assets/diagram-primary.svg">
</p>
