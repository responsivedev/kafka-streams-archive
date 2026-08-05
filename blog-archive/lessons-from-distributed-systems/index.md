# Four Horsemen: Lessons from Distributed System Apocalypse

*Almog Gavra and Apurva Mehta · March 31, 2025 · 8 min read*

Learning from history is the best way to guide future decisions. This post examines four architectural mistakes that distributed systems can make that prevents their longterm success.

![Four Horsemen: Lessons from Distributed System Apocalypse](img/hero-image.png)

---

All distributed systems begin with promise. The ones that endure are those that evolve. The rest become cautionary tales.

I believe Stream Processing today is at this inflection point. It has had tremendous success in specific niches, but risks becoming a technology that came and went without lasting impact.

Personally, I would be saddened by this outcome. The promise of stream processing has only scratched the surface of “possible”. Instead, I imagine a world where digital feedback loops are as high fidelity as those in the physical world: where public transit reroutes itself based on realtime pedestrian flows, where your smart thermostat adjusts the sensors for temperature based on the rooms your family are in, where financial transactions are secured in seconds, not days.

What prevents this promise from reality?

There is no one root cause, but as an engineer I gravitate to understanding the technology limitations that limit the potential of infrastructure. I’ve always believed that the best way to understand limitations is to examine the successes and failures of other, similar systems.

During my research, I discovered four patterns that plagued modern distributed systems. In this post, I present them as the four horseman that signal the decline of once-mighty technologies:

## The First Horseman: Coupled Storage & Compute

> *I watched as the Lambda Function opened the first seal and I heard one of the four living creatures say in a voice like thunder, “Come!” So I looked, and saw a white horse and its rider held a spinning disk. **And he bound storage with compute, riding out to destabilize and rebalance.***
>

I start my story where many stories begin: Hadoop. The mother of modern distributed systems, it is a story of both innovation and decline.

While many factors contributed to its eventual decline, one key limitation of Hadoop was the coupling of MapReduce (its compute layer) with HDFS (the distributed storage layer). This limitation meant that if a job required more compute, it was necessary to provision more storage. This is not only a demerit to its cost efficiency, but it made scaling each component dynamically challenging: adding new computational resources required expensive data redistribution (also known as “rebalancing”) to take advantage of the expanded capacity.

The systems that ultimately replaced Hadoop all separate storage from compute, allowing their operators an additional degree of freedom with their resource allocation.

Redshift’s journey is somewhat different from Hadoop. While its initial design also relied on a monolithic architecture that tightly coupled storage and compute, it was able to break out of that initial blueprint. [Redshift 2.0](https://assets.amazon.science/4b/37/223ac61e450898244a31bed53734/amazon-redshift-re-invented.pdf), unlike its predecessor, stores data to S3, only using local SSDs to serve as caches for frequently accessed data. This allows for an entirely elastic compute tier that can expand and contract capacity without shuffling or rebalancing any data.

This new foundation for Redshift went beyond granting boons to efficiency and reliability, it also enabled native integrations with a wider ecosystem. Having a query engine that can process data in S3 enabled Spectrum, which is Redshift’s capability to query data stored in S3 on open data formats such as Parquet without having ingested them through Redshift. The improved computation flexibility enabled AQUA, a hardware accelerator for Redshift that can leverage FPGAs on demand to process certain intense computational queries faster.

Kafka Streams carries the burden of its design heritage—it was built in an era when stream processing was just budding, and access to cloud storage was not a given. At Responsive, we see the writing on the wall: the future lies in separating state storage from compute, eliminating the constraints that prevent elasticity and efficiency.

[And it shall be done](https://www.confluent.io/events/kafka-summit-london-2024/its-a-trap-solving-restoration-with-custom-kafka-streams-state-stores/).

## The Second Horseman: Complex Capacity Planning

> And when the second seal was opened, I heard the second creature say “Come!” Then another horse went forth. It was bright red, and **its rider was granted permission to break all existing capacity planning assumptions**.
>

I work in a unique professional, one where teams publicly showcase their own failures: there are numerous public postmortem reports that analyze failures of massive production deployments.

Through this, I learned this lesson: If you must capacity plan to remain online, you have already failed.

With distributed systems, predicting the needs of a system over time is nearly impossible. In stream processing the problem is exacerbated since seasonality is measured on an hourly basis, not monthly. Instead, modern systems must be built with elasticity, utilizing reserved capacity and resource limits only as instruments for monetary efficiency.

Listen to the story of a whale swallowing legacy infrastructure: by 2014 Twitter (for those of us old enough to remember it by that name) was infamous for high profile failings. The FIFA World Cup had caused a surge of realtime tweets that broke the capacity planning model Twitter had in place, resulting in multiple outages during its opportunity to take center stage. This caused the team [to rearchitect](https://blog.x.com/engineering/en_us/a/2013/new-tweets-per-second-record-and-how) their systems, focusing on elasticity of disparate services so that bottlenecks could be identified and scaled independently, and in realtime. Without any indication that such an event could happen, a movie from the 1980s streamed in Japan caused a new record influx of tweets. But due to their new independence from capacity planning, Twitter was prepared to scale without having planned for that capacity.

Similarly, in 2012 Netflix [embarked to end their monolithic Oracle RDBMS](https://www.youtube.com/watch?v=tDsVNd8ewnQ) and replace it with independently scalable components. As a result of this effort they were able to not only improve availability, but their autoscaling was able to reduce infrastructure costs [by over 50%](https://youtu.be/tDsVNd8ewnQ?si=_as9ClaiDZ3ccUKQ&t=1504) by scaling with the seasonality of viewership.

This got me thinking… What can be done to the humble stream processor so that it too can scale elastically and avoid the perils of imperfect capacity planning?

Once storage and compute have been decoupled into independent components, Kafka Streams can not only scale elastically and automatically — it can surpass traditional systems in flexibility. Stream processors are unique in that their failure modes naturally result in graceful degradation of lag accumulation as opposed to failed or stalled requests. This allows [a sufficiently advanced streaming autoscaling intelligence](https://docs.responsive.dev/control-plane) to define service level goals that optimize for either efficiency or throughput without ever fearing a complete processing failure.

[And it shall be done.](https://www.responsive.dev/blog/kafka-streams-control-plane)

## The Third Horseman: Degraded Developer Experience

> And when the third seal was opened, I heard the third creature say “Come!” Then I looked and saw a black horse, and its rider held in his hand a compiled JAR. And I heard a what sounded like a voice among the four living creatures, saying: “**Read the technical documentation, and do not touch the system without expertise.**”
>

A lesson I have experienced firsthand is that a system’s power is meaningless if developers can’t use it. History is littered with brilliant technologies that failed because they demanded too much expertise.

We return to Hadoop, lessons from which are evergreen despite the decline of the technology itself: the developer experience is frequently cited as another contributing factor for its fall. Cloudera & Hortonworks poured significant investment into making the administration and operation of Hadoop simpler, but the developer experience remained subpar. CI/CD involved manually uploading, versioning and resolving JARs. Debugging required deep knowledge of YARN and the different components in the Hadoop architecture. Testing required local deployments of mocked infrastructure.

Contrast this with MongoDB — a database initially ridiculed for its operational problems (remember “[MongoDB is Web Scale](https://www.youtube.com/watch?v=b2F-DItXtZs&t=2s)”?) flourished because it prioritized developer experience. It made data access easy, provided intuitive APIs, and abstracted away operational complexity. Developers flocked to it, and over time, it grew into a robust database that holds its own amongst giants.

Kafka Streams has a leg up against this horseman—it is not a cluster to operate, nor a service to provision, but a library that integrates seamlessly with your existing ecosystem. It compiles with your application. It deploys like your application. It logs like your application. And because of that, it has the potential to feel as natural and intuitive as any modern web framework or HTTP client.

But Kafka Streams lacks many of the affordances developers expect today: built-in observability, local testing that mirrors production, and patterns for managing state, fault tolerance, and retries. These gaps don’t just slow teams down—they threaten to erode the simplicity that sets Kafka Streams apart. At Responsive, we refuse to let that happen. We will preserve the elegance of the “just a library” model, while surrounding it with the tools and abstractions that developers need to thrive. Developer experience is not a concession to power—it is a prerequisite for adoption and success.

[And it shall be done.](https://www.responsive.dev/blog/kafka-streams-is-an-application-framework)

## The Fourth Horseman: Ignoring the Cloud

> And when the fourth seal was opened, I heard the voice of the fourth creature say “Come!” Then I looked and saw a pale green horse. Its rider’s name was S3, and GCS followed close behind. **And they were given authority over non cloud-native infrastructure, to kill by forcing design patterns from legacy infrastructure onto the earth.**
>

Looking at the systems of the past can only teach us so much, so I must also look to the present and see where technologies break new ground.

There are numerous example of modern data systems breaking from the mold of traditional distributed systems in that they openly rely on the presence of a select few cloud native primitives. Namely, they rely on Object Storage and Kubernetes.

Neon and Warpstream all delegate persistence to S3 in order to reduce costs and complexity in their operational layer as a way to challenge their predecessors (Postgres and Kafka respectively). A naive implementation risks unacceptable latencies or enormous S3 API bills, so each of these systems has had to implement an intelligent (stateless, except for cache) intermediary between S3 and their compute layer. SlateDB, another cloud-native database system with significant contributions from Responsive, implements this intermediate layer by buffering writes in an in-memory buffer and only writing to S3 at a configurable interval and caching blocks on disk to reduce object store reads during steady state operation.

I can glimpse into the future and see that Kafka Streams can adapt to this architecture as well. While other stream processors traditionally require bespoke infrastructure and dedicated schedulers (such as Flink’s Job Manager), Kafka Streams’ design—embedded, lightweight, and decentralized—makes it uniquely suited to a world where compute is ephemeral and storage is infinite. But to thrive in this world, it must shed the assumptions of the datacenter era. It must learn to speak the language of the cloud: relying on object storage for persistence, Kubernetes for scaling and support RPCs to interact with other microservices.

At Responsive, we are building this vision: a Kafka Streams engine reborn for the cloud, one that embraces its primitives and thrives within its constraints.

[And it shall be done.](https://www.responsive.dev/blog/why-slatedb-for-kafka-streams)

## The Final Seal

The Four Horsemen have ridden through the distributed systems landscape, leaving behind a trail of abandoned architectures and hard-earned lessons. The systems that endured did not do so by accident; they adapted, shedding the burdens of their past and embracing new paradigms.

Kafka Streams has survived where others have stumbled, but survival is not enough—it must evolve. The path forward is clear: decouple storage and compute, build an intelligent control plane, preserve its developer experience, and embrace the cloud-native future.

At Responsive, we are forging that future. We refuse to let Kafka Streams be another cautionary tale in the history of distributed systems. Instead, we are rearchitecting it into a platform that scales effortlessly, optimizes costs, and empowers developers without unnecessary complexity.

The Four Horsemen are harbingers of obsolescence. We choose a different fate.

[And it has been done.](https://www.responsive.dev/)
