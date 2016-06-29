This is a demo application to test the OffHeap capabilities of Apache Ignite.
Goals:
 #1 Store huge amount of data off-heap (50+ GB).
 #2 Resize off-heap cache at runtime
 #3 Improve config to deactivate as many features as possible (only the OffHeap part is important for the first prototype)

Start it with VM options: -Xms768m -Xmx768m -XX:MaxDirectMemorySize=2048m