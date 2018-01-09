@REM ----------------------------------------------------------------------------
@REM Copyright 2001-2004 The Apache Software Foundation.
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM ----------------------------------------------------------------------------
@REM

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\etc;"%REPO%"\org\apache\flink\flink-core\1.3.1\flink-core-1.3.1.jar;"%REPO%"\org\apache\flink\flink-annotations\1.3.1\flink-annotations-1.3.1.jar;"%REPO%"\org\apache\flink\flink-metrics-core\1.3.1\flink-metrics-core-1.3.1.jar;"%REPO%"\org\apache\commons\commons-lang3\3.3.2\commons-lang3-3.3.2.jar;"%REPO%"\com\esotericsoftware\kryo\kryo\2.24.0\kryo-2.24.0.jar;"%REPO%"\com\esotericsoftware\minlog\minlog\1.2\minlog-1.2.jar;"%REPO%"\org\objenesis\objenesis\2.1\objenesis-2.1.jar;"%REPO%"\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar;"%REPO%"\org\apache\commons\commons-compress\1.4.1\commons-compress-1.4.1.jar;"%REPO%"\org\tukaani\xz\1.0\xz-1.0.jar;"%REPO%"\org\apache\avro\avro\1.7.7\avro-1.7.7.jar;"%REPO%"\org\codehaus\jackson\jackson-core-asl\1.9.13\jackson-core-asl-1.9.13.jar;"%REPO%"\org\codehaus\jackson\jackson-mapper-asl\1.9.13\jackson-mapper-asl-1.9.13.jar;"%REPO%"\com\thoughtworks\paranamer\paranamer\2.3\paranamer-2.3.jar;"%REPO%"\org\slf4j\slf4j-api\1.7.7\slf4j-api-1.7.7.jar;"%REPO%"\com\google\code\findbugs\jsr305\1.3.9\jsr305-1.3.9.jar;"%REPO%"\org\apache\flink\force-shading\1.3.1\force-shading-1.3.1.jar;"%REPO%"\org\apache\flink\flink-java\1.3.1\flink-java-1.3.1.jar;"%REPO%"\org\apache\flink\flink-shaded-hadoop2\1.3.1\flink-shaded-hadoop2-1.3.1.jar;"%REPO%"\xmlenc\xmlenc\0.52\xmlenc-0.52.jar;"%REPO%"\commons-codec\commons-codec\1.4\commons-codec-1.4.jar;"%REPO%"\commons-io\commons-io\2.4\commons-io-2.4.jar;"%REPO%"\commons-net\commons-net\3.1\commons-net-3.1.jar;"%REPO%"\javax\servlet\servlet-api\2.5\servlet-api-2.5.jar;"%REPO%"\org\mortbay\jetty\jetty-util\6.1.26\jetty-util-6.1.26.jar;"%REPO%"\com\sun\jersey\jersey-core\1.9\jersey-core-1.9.jar;"%REPO%"\commons-el\commons-el\1.0\commons-el-1.0.jar;"%REPO%"\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar;"%REPO%"\com\jamesmurty\utils\java-xmlbuilder\0.4\java-xmlbuilder-0.4.jar;"%REPO%"\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;"%REPO%"\commons-configuration\commons-configuration\1.7\commons-configuration-1.7.jar;"%REPO%"\commons-digester\commons-digester\1.8.1\commons-digester-1.8.1.jar;"%REPO%"\com\jcraft\jsch\0.1.42\jsch-0.1.42.jar;"%REPO%"\commons-beanutils\commons-beanutils-bean-collections\1.8.3\commons-beanutils-bean-collections-1.8.3.jar;"%REPO%"\commons-daemon\commons-daemon\1.0.13\commons-daemon-1.0.13.jar;"%REPO%"\javax\xml\bind\jaxb-api\2.2.2\jaxb-api-2.2.2.jar;"%REPO%"\javax\xml\stream\stax-api\1.0-2\stax-api-1.0-2.jar;"%REPO%"\javax\activation\activation\1.1\activation-1.1.jar;"%REPO%"\org\apache\commons\commons-math3\3.5\commons-math3-3.5.jar;"%REPO%"\org\apache\flink\flink-streaming-java_2.10\1.3.1\flink-streaming-java_2.10-1.3.1.jar;"%REPO%"\org\apache\flink\flink-runtime_2.10\1.3.1\flink-runtime_2.10-1.3.1.jar;"%REPO%"\io\netty\netty-all\4.0.27.Final\netty-all-4.0.27.Final.jar;"%REPO%"\org\javassist\javassist\3.18.2-GA\javassist-3.18.2-GA.jar;"%REPO%"\org\scala-lang\scala-library\2.10.4\scala-library-2.10.4.jar;"%REPO%"\com\data-artisans\flakka-actor_2.10\2.3-custom\flakka-actor_2.10-2.3-custom.jar;"%REPO%"\com\typesafe\config\1.2.1\config-1.2.1.jar;"%REPO%"\com\data-artisans\flakka-remote_2.10\2.3-custom\flakka-remote_2.10-2.3-custom.jar;"%REPO%"\io\netty\netty\3.8.0.Final\netty-3.8.0.Final.jar;"%REPO%"\com\google\protobuf\protobuf-java\2.5.0\protobuf-java-2.5.0.jar;"%REPO%"\org\uncommons\maths\uncommons-maths\1.2.2a\uncommons-maths-1.2.2a.jar;"%REPO%"\com\data-artisans\flakka-slf4j_2.10\2.3-custom\flakka-slf4j_2.10-2.3-custom.jar;"%REPO%"\org\clapper\grizzled-slf4j_2.10\1.0.2\grizzled-slf4j_2.10-1.0.2.jar;"%REPO%"\com\github\scopt\scopt_2.10\3.5.0\scopt_2.10-3.5.0.jar;"%REPO%"\org\apache\zookeeper\zookeeper\3.4.6\zookeeper-3.4.6.jar;"%REPO%"\jline\jline\0.9.94\jline-0.9.94.jar;"%REPO%"\junit\junit\3.8.1\junit-3.8.1.jar;"%REPO%"\com\twitter\chill_2.10\0.7.4\chill_2.10-0.7.4.jar;"%REPO%"\com\twitter\chill-java\0.7.4\chill-java-0.7.4.jar;"%REPO%"\org\apache\sling\org.apache.sling.commons.json\2.0.6\org.apache.sling.commons.json-2.0.6.jar;"%REPO%"\org\apache\flink\flink-clients_2.10\1.3.1\flink-clients_2.10-1.3.1.jar;"%REPO%"\org\apache\flink\flink-optimizer_2.10\1.3.1\flink-optimizer_2.10-1.3.1.jar;"%REPO%"\commons-cli\commons-cli\1.3.1\commons-cli-1.3.1.jar;"%REPO%"\org\apache\flink\flink-connector-twitter_2.10\1.3.1\flink-connector-twitter_2.10-1.3.1.jar;"%REPO%"\com\fasterxml\jackson\core\jackson-core\2.8.9\jackson-core-2.8.9.jar;"%REPO%"\com\fasterxml\jackson\core\jackson-databind\2.8.9\jackson-databind-2.8.9.jar;"%REPO%"\com\fasterxml\jackson\core\jackson-annotations\2.8.9\jackson-annotations-2.8.9.jar;"%REPO%"\de\undercouch\bson4jackson\2.7.0\bson4jackson-2.7.0.jar;"%REPO%"\org\apache\flink\flink-connector-kafka-0.10_2.10\1.3.2\flink-connector-kafka-0.10_2.10-1.3.2.jar;"%REPO%"\org\apache\flink\flink-connector-kafka-0.9_2.10\1.3.2\flink-connector-kafka-0.9_2.10-1.3.2.jar;"%REPO%"\org\apache\flink\flink-connector-kafka-base_2.10\1.3.2\flink-connector-kafka-base_2.10-1.3.2.jar;"%REPO%"\org\apache\kafka\kafka-clients\0.10.2.1\kafka-clients-0.10.2.1.jar;"%REPO%"\net\jpountz\lz4\lz4\1.3.0\lz4-1.3.0.jar;"%REPO%"\org\xerial\snappy\snappy-java\1.1.2.6\snappy-java-1.1.2.6.jar;"%REPO%"\org\mongodb\mongodb-driver\3.4.2\mongodb-driver-3.4.2.jar;"%REPO%"\org\mongodb\bson\3.4.2\bson-3.4.2.jar;"%REPO%"\org\mongodb\mongodb-driver-core\3.4.2\mongodb-driver-core-3.4.2.jar;"%REPO%"\org\grouplens\lenskit\lenskit-all\2.2.1\lenskit-all-2.2.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-svd\2.2.1\lenskit-svd-2.2.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-slopeone\2.2.1\lenskit-slopeone-2.2.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-core\2.2.1\lenskit-core-2.2.1.jar;"%REPO%"\net\mikera\vectorz\0.41.2\vectorz-0.41.2.jar;"%REPO%"\us\bpsm\edn-java\0.4.4\edn-java-0.4.4.jar;"%REPO%"\net\mikera\randomz\0.3.0\randomz-0.3.0.jar;"%REPO%"\net\mikera\mathz\0.3.0\mathz-0.3.0.jar;"%REPO%"\com\google\guava\guava\18.0\guava-18.0.jar;"%REPO%"\org\grouplens\grapht\grapht\0.10.0\grapht-0.10.0.jar;"%REPO%"\javax\inject\javax.inject\1\javax.inject-1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-api\2.2.1\lenskit-api-2.2.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-data-structures\2.2.1\lenskit-data-structures-2.2.1.jar;"%REPO%"\it\unimi\dsi\fastutil\6.6.1\fastutil-6.6.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-knn\2.2.1\lenskit-knn-2.2.1.jar;"%REPO%"\com\google\code\findbugs\annotations\2.0.1\annotations-2.0.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-eval\2.2.1\lenskit-eval-2.2.1.jar;"%REPO%"\org\grouplens\lenskit\lenskit-groovy\2.2.1\lenskit-groovy-2.2.1.jar;"%REPO%"\joda-time\joda-time\2.3\joda-time-2.3.jar;"%REPO%"\org\apache\ant\ant\1.8.4\ant-1.8.4.jar;"%REPO%"\org\apache\ant\ant-launcher\1.8.4\ant-launcher-1.8.4.jar;"%REPO%"\org\codehaus\groovy\groovy-all\2.1.5\groovy-all-2.1.5.jar;"%REPO%"\org\hamcrest\hamcrest-library\1.3\hamcrest-library-1.3.jar;"%REPO%"\org\hamcrest\hamcrest-core\1.3\hamcrest-core-1.3.jar;"%REPO%"\org\grouplens\lenskit\lenskit-predict\2.2.1\lenskit-predict-2.2.1.jar;"%REPO%"\SSII\DataStreaming\0.0.1-SNAPSHOT\DataStreaming-0.0.1-SNAPSHOT.jar
set EXTRA_JVM_ARGUMENTS=
goto endInit

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS% %EXTRA_JVM_ARGUMENTS% -classpath %CLASSPATH_PREFIX%;%CLASSPATH% -Dapp.name="producer" -Dapp.repo="%REPO%" -Dbasedir="%BASEDIR%" data.streaming.test.TestFlinkKafkaProducer %CMD_LINE_ARGS%
if ERRORLEVEL 1 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=1

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@endlocal

:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
