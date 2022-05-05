 docker run -d \
   --name td  \
   --net=host \
   ## do not use this as hostname for the container set right FQDN in taos.conf instead
   --hostname=localhost \
   tdengine/tdengine