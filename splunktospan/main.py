from splunklib.client import connect
from splunklib.results import ResultsReader
from .span import LogParser

try:
    import utils
except ImportError:
    raise Exception("Add the SDK repository to your PYTHONPATH to run the examples "
                    "(e.g., export PYTHONPATH=~/splunk-sdk-python.")

# for examples of tailing off the splunk API see
# https://github.com/splunk/splunk-sdk-python/blob/master/examples/stail.py
# and https://github.com/splunk/splunk-sdk-python/blob/master/examples/follow.py
#
def example_main():
    ls_component_key = "lightstep.component_name"
    join_guid = "guid:correlation_id"
    tags_to_rewrite = {"correlation-id": join_guid, "cid": join_guid, "component": ls_component_key}

    # splunk set up
    opts = utils.parse(sys.argv[1:], {}, ".splunkrc", usage=usage)
    if len(opts.args) != 1:
        utils.error("Search expression required", 2)
    search = opts.args[0]
    service = connect(**opts.kwargs)

    regex = re.compile("(?P<start_time>.+) (?P<component>.+) (?P<operation>.+): (?P<tags>.*)")
    log_parser = LogParser(regex)
    log_parser.downcase_keys = True
    log_parser.duration_keys = ["latencyMillis", "elapsedMillis", "duration"]

    result = service.get(
            "search/jobs/export",
            search=search,
            earliest_time="rt",
            latest_time="rt",
            search_mode="realtime")

    for result in results.ResultsReader(result.body):
        # I'm not sure what will actually be used here since ResultReader can return
        # either a dictionary or message. I imagine a dict=>ParsedLog should be
        # straightforward to write.
        # https://github.com/splunk/splunk-sdk-python/blob/master/splunklib/results.py#L170
        if result is None or isinstance(result, dict):
            break
        parsed = log_parser.parse_line(result.message)
        parsed.rewrite_tags(tags_to_rewrite)
        if int(parsed.tags["status"]) >= 300:
            parsed.tags["error"] = True
        parsed.to_span()

