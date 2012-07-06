In order to run the tests from Lucene using the HBaseDirectory implementation in HBaluin have to follow these steps:
  0. First get Lucene project from the apache repository:

       git clone https://github.com/apache/lucene-solr.git

     or if you prefer svn:
 
       svn co http://svn.apache.org/repos/asf/lucene/dev/trunk lucene_trunk

     then build the project(it's based on ant and ivy):

       ant ivy-bootstrap #downloads and installs ivy

       ant compile #compiles the project

       ant test #runs tests, takes some time, they all pass

       ant idea or ant eclipse for IDE integration

  1. For faster build and dependencies in the ant based Lucene project we use the mvn assembler plugin to create a jar with all dependencies:

     mvn clean package -DskipTests

  2. Create the lib directory inside the core lucene and add the jar there:

     cp target/HBaluin-0.0.2-SNAPSHOT-jar-with-dependencies.jar ~/workspace/lucene-solr/lucene/core/lib/

  3. Modify the build.xml file in lucene/core and add under "test.classpath" path the following:

     <fileset file="lib/HBaluin-0.0.2-SNAPSHOT-jar-with-dependencies.jar"/>

  4. Run using ant from lucene-solr/lucene/:

     ant test-core -Dtests.directory=org.apache.james.mailbox.lucene.hbase.HBaseDirectory
