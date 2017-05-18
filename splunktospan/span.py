import time
from datetime import timedelta
from datetime import datetime
from dateutil.parser import parse as rfc3339_parse


required_groups = ["operation", "start_time", "tags"]
tracers = {}

class ParsedLog(object):
    """
    ParsedLog is an intermediate representation between an arbritrary log and span.
    If an opentracing tracer is not provided, it defaults to the global tracer
    in the opentracing package.
    """

    def __init__(self):
        self.operation = None
        self.start_time = None
        self.end_time = None
        self.tags = {}
        self.tracer = None

    def rewrite_tags(self, rewrite_dict, delete=False):
        """
        rewrite_tags will go through the tags and give them new keys.
        Arguments:
            rewrite_dict (dict): mapping of existing keys to new keys.
            delete (bool): deletes the existing key after writing to the new key.
        """
        for key, new_key in rewrite_dict.iteritems():
            if key in self.tags:
                value = self.tags[key]
                self.tags[new_key] = value
                if delete:
                    del self.tags[key]

    def to_span(self):
        if "guid:correlation_id" in self.tags.keys():
            sp = self.tracer.start_span(operation_name=self.operation_name,
                                    tags=self.tags,
                                    start_time=self.start_time)
            sp.finish(finish_time=self.end_time)
        else:
            print("Skipping because there's no guid:correlation_id in tags")

class DictParser(object):
    """
    DictParser parses out a dict into a ParsedLog.
    """
    def __init__(self):
        self.downcase_keys = False
        self.timestamp_keys = ['_time']
        self.operation_keys = ['activity', 'path']
        self.duration_keys = ["latencyMillis", "elapsedMillis", "duration", 'dur']
        self.downcase_keys = True

    def parse_dict(self, d, end=None):
        if self.downcase_keys:
            dict_copy = d.copy()
            d = {}
            for key, value in dict_copy.iteritems():
                d[key.lower()] = value

        dur = None
        start = None
        operation = None
        for key in self.operation_keys:
            if key in d:
                operation = d[key]
                break
        if operation is None:
            raise Exception("No operation name found in dict, check your operation_keys list", d)

        for key in self.duration_keys:
            if key in d:
                dur = timedelta(milliseconds=int(d[key]))
                break
        if dur is None:
            raise Exception("No duration found in dict, check your duration_keys list", d)

        #for key in self.timestamp_keys:
        #    if key in d:
        #        end = rfc3339_parse(d[key])
        #        print key
        #        print end
        #        if end is not None:
        #            print end.dst()
        #        else:
        #            print d
        #        break
        end = rfc3339_parse(d["_raw"].split(" ")[0])
        #if end is None:
        #    print d
        if start is None:
            start = end - dur
        else:
            end = start + dur

        log = ParsedLog()
        log.operation_name = operation
        log.start_time = time.mktime(start.utctimetuple()) + start.microsecond / 1E6
        log.end_time = time.mktime(end.utctimetuple()) + end.microsecond / 1E6
        log.tags = d
        return log


class LogParser(object):
    """
    LogParser parses a log line into a ParsedLog.

    Arguments:
    regex (re.RegexObject): LogParser requires a valid regex to actually parse the lines.
                            Three capture groups are required: start_time, operation, and tags.
                            All other capture groups are added into the ParsedLogs.tag dict,
                            overwriting any tags pulled out from the <tags> capture group.

    tracer (opentracing.Tracer): If no tracer is provided, the global tracer is used.
    duration_keys (list<string>): These specify any tag that is actually the duration of the log.
    downcase_keys (bool): If True, all tag keys are lower cased.
    """
    def __init__(self, regex, tracer=None):
        self.tracer = tracer
        self.downcase_keys = False
        self.duration_keys = ["dur"]
        if not self.is_valid_regex(regex):
            raise Exception("invalid regex used for log parser")
        self.regex = regex

    def is_valid_regex(self, regex):
        groupindex = regex.groupindex
        if "start_time" not in groupindex:
            return False
        if "operation" not in groupindex:
            return False
        if "tags" not in groupindex:
            return False
        return True

    def trim_str(self, value):
        # remove values wrapped with quotation marks.
        if value.startswith('"') and value.endswith('"'):
            value = value[1:len(value)-1]
        value = value.strip()
        return value

    def extract_tags(self, tags_str, token="="):
        """
        extract_tags pulls out KV pairs from a string that are seperated by a token.
        Returns a dictionary of type string, string
        """

        tags = {}
        for tag in tags_str.split(" "):
            kv = tag.split(token)
            if len(kv) != 2:
                continue
            key = kv[0]
            value = self.trim_str(kv[1])
            if self.downcase_keys:
                key = key.lower()

            tags[key] = value

        return tags

    # extract_duration assumes that all values are millis
    def extract_duration(self, tags):
        for key in self.duration_keys:
            if key in tags:
                millis = int(tags[key])
                return timedelta(milliseconds=millis)
        return None

    def parse_line(self, line):
        match = self.regex.match(line)
        index = self.regex.groupindex
        if match is None:
            raise Exception("could not match line against provided regex")

        tags = self.extract_tags(match.group("tags"))

        # Put any other capture groups into the tag set.
        for key in index:
            if key in required_groups:
                continue
            tags[key] = match.group(key)

        duration = self.extract_duration(tags)
        if duration is None:
            raise Exception("could not find duration")
        log = ParsedLog(tracer=self.tracer)
        log.operation = match.group("operation")
        log.start_time = rfc3339_parse(match.group("start_time"))
        log.end_time = log.start_time+duration
        log.tags = tags

        return log
