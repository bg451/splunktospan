import unittest
import re
import splunktospan

line = """2017-04-17T11:41:47.926046-07:00 app2.javaserver shoppingcart-service: severity="INFO " correlation-ID="3c1c7b15" sid="" thread="http-8080-1" status="SUCCESS" method="DELETE" path="/rest/carts?guid=abc123" status="200" duration="109" REDACTED"""

line_regex = re.compile("(?P<start_time>.+) (?P<component>.+) (?P<operation>.+): (?P<tags>.*)")

class TestLogParser(unittest.TestCase):

    def test_parse_line(self):
        l = splunktospan.LogParser(line_regex)
        log = l.parse_line(line)
        assert log.operation == "shoppingcart-service"
        assert log.start_time is not None
        assert log.tags['thread'] == 'http-8080-1'
        assert log.tags['component'] == 'app2.javaserver'

    def test_is_valid_regex(self):
        l = splunktospan.LogParser(line_regex)
        assert l.is_valid_regex(line_regex) == True

        regex = re.compile("(?P<timestamp>.+) (?P<component>.+) (?P<operation>.+): (?P<tags>.*)")
        assert l.is_valid_regex(regex) == False

    def test_extract_tags(self):
        l = splunktospan.LogParser(line_regex)
        tags_str = """httpStatusCode="200" elapsedMillis="109" REDACTED"""
        tags = l.extract_tags(tags_str)
        assert len(tags) == 2
        assert tags["httpStatusCode"] == '200'
        assert tags["elapsedMillis"] == '109'

    def test_extract_tags_lower(self):
        l = splunktospan.LogParser(line_regex)
        l.downcase_keys = True
        tags_str = """httpStatusCode=200"""
        tags = l.extract_tags(tags_str)
        assert "httpStatusCode".lower() in tags

