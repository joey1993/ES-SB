from elasticsearch import Elasticsearch
import os

# Home
HOME_DIR = os.getenv("HOME")
client = Elasticsearch([{'host':'localhost','port':9201}])


g = open('/home/joey/Dropbox/LAB/category.txt','r')
line = g.readline()
cat_dict = {}
while line != '':
    vec = line.replace('\n','').split('\t')
    if not cat_dict.has_key(vec[0]):
        cat_dict[vec[0]]=vec[1]
    line = g.readline()
g.close()


f = open('/home/joey/Dropbox/Seg_sents.txt','r')
line = f.readline()
i = 1
while line != '':
    if 'cause' in line:
        vec = line.replace('\n','').split('$')
        content = {}
        tags = []
        content['title'] = vec[0]
        content['sent'] = vec[2]
        sen_vec = vec[2].split(' ')
        for item in sen_vec:
            if cat_dict.has_key(item):
                if cat_dict[item] == u'disease' or cat_dict[item] == u'symptom':  
                    tags.append(cat_dict[item])
            else:
                tags.append('NA')
        content['tags'] = tags
        client.index(index='baidu',doc_type='cause',id=i, body=content)
        i = i + 1
    line = f.readline()
print i
f.close()















# class IndexWorker(parallel.Worker):
#     def __init__(self, queue, corenlp, logger):
#         parallel.Worker.__init__(self)
#         self.queue = queue
#         self.corenlp = corenlp
#         self.logger = logger


#     def work(self):
#         self.logger.debug("Process %d has begun.", self.process.pid)

#         while True:

#             # path of next extracted file
#             path = self.queue.get()
#             self.logger.info('Now processing file: %s', path)
            
#             # acquire file handler
#             with open(path, 'r') as f:

#                 # loop while a doc is available
#                 (xml, text) = parser.scan_next(f)
#                 while xml:

#                     # parse
#                     parse = parser.combined_parse(xml, text, self.corenlp)
#                     page = parse['page']
#                     sentences = parse['sentences']

#                     # index
#                     es.index_page(page)
#                     for sentence in sentences:
#                         es.index_sentence(sentence)

#                     self.logger.debug("Indexed article %d: \"%s\"",
#                                       page['id'],
#                                       page['title'])

#                     # continue
#                     (xml, text) = parser.scan_next(f)
            
#             # notify queue
#             self.queue.task_done()
#             self.logger.info('Done processing file: %s', path)


# def main(args):
#     print "Running..."

#     # get extracted wikipedia file pathnames
#     subdirs = io.list_directories(config.WIKIPEDIA_EXTRACTED_DIR)
#     if args.letters:
#         subdirs = [p for p in subdirs if p[-2] in config.WIKIPEDIA_SUB_DIR_PREFIXES]
#     pathnames = []
#     for sb in subdirs:
#         pathnames.extend(io.list_files(sb))
#     pathnames.sort()
    
#     # create thread-safe queue
#     queue = parallel.create_queue(pathnames)

#     # create corenlp process
#     corenlp = parser.get_parser()

#     # create workers
#     workers = []
#     for i in xrange(args.threads):
#         logger = log.create_logger('LOGGER %d' % i, 'log_%d.log' % i)
#         worker = IndexWorker(queue, corenlp, logger)
#         workers.append(worker)

#     # begin
#     for worker in workers:
#         worker.start()

#     # block until all files have been processed
#     queue.join()

#     print "Done!"




# if __name__ == '__main__':
#     argparser = argparse.ArgumentParser()

#     argparser.add_argument('-t', '--threads',
#                            default=2,
#                            type=int,
#                            help='Number of threads used to index')

#     args = argparser.parse_args()

#     main(args)
