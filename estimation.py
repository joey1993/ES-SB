from utils import io
from classes import classes
from elastic import es
from config import config

def main():
    pathnames = []
    pathnames.extend(io.list_files(config.SNOWBALL_CURRENT_CAUSE_DIR))
    pathnames.sort()
    i = 0

    for path in pathnames:
        print 'file ' + i
        
        f = open(path,'r')
        filename = str('ES_') + str(path[48:])
        g = open(filename,'w')
        line = f.readline()
        while line != '':
            tuple = line.replace('\n','').split(' cause ')
            subj = tuple[0].decode('utf-8')
            obj = tuple[1].decode('utf-8')
            tup = classes.CandidateTuple(u'cause',
                                         subj,
                                         obj,
                                         u'disease',
                                         u'disease',
                                         1.0)
            hit = es.get_sentences_containing(tup, 0, 1)
            source = hit[0]['_source']
            string = subj + '\t' + obj + '\t' + source[u'sent']
            g.write(string.encode('utf-8')+'\n')
            line = f.readline()
        f.close()
        g.close()

if __name__ == '__main__':
    main()
