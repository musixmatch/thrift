// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "ThreadsTest.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TTransportUtils.h>
#include <thrift/concurrency/Monitor.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>

using boost::shared_ptr;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::concurrency;


class ThreadsTestHandler : virtual public ThreadsTestIf {
 public:
  ThreadsTestHandler() {
    // Your initialization goes here
  }

  int32_t threadOne(const int32_t sleep) {
    // Your implementation goes here
    printf("threadOne\n");
    go2sleep(1, sleep);
    return 1;
  }

  int32_t threadTwo(const int32_t sleep) {
    // Your implementation goes here
    printf("threadTwo\n");
    go2sleep(2, sleep);
    return 1;
  }

  int32_t threadThree(const int32_t sleep) {
    // Your implementation goes here
    printf("threadThree\n");
    go2sleep(3, sleep);
    return 1;
  }

  int32_t threadFour(const int32_t sleep) {
    // Your implementation goes here
    printf("threadFour\n");
    go2sleep(4, sleep);
    return 1;
  }

  int32_t stop() {
    printf("stop\n");
    server_->stop();
    return 1;
  }

  void setServer(boost::shared_ptr<TServer> server) {
    server_ = server;
  }

protected:
  void go2sleep(int thread, int seconds) {
    Monitor m;
    for (int i = 0; i < seconds; ++i) {
      fprintf(stderr, "Thread %d: sleep %d\n", thread, i);
      try {
        m.wait(1000);
      } catch(TimedOutException& e) {
      }
    }
    fprintf(stderr, "THREAD %d DONE\n", thread);
  }

private:
  boost::shared_ptr<TServer> server_;

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<ThreadsTestHandler> handler(new ThreadsTestHandler());
  shared_ptr<TProcessor> processor(new ThreadsTestProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  /*
  shared_ptr<ThreadManager> threadManager =
    ThreadManager::newSimpleThreadManager(10);
  shared_ptr<PosixThreadFactory> threadFactory =
    shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();

  shared_ptr<TServer> server =
    shared_ptr<TServer>(new TThreadPoolServer(processor,
                                              serverTransport,
                                              transportFactory,
                                              protocolFactory,
                                              threadManager));
  */

  shared_ptr<TServer> server =
    shared_ptr<TServer>(new TThreadedServer(processor,
                                            serverTransport,
                                            transportFactory,
                                            protocolFactory));

  handler->setServer(server);

  server->serve();

  fprintf(stderr, "done.\n");

  return 0;
}

