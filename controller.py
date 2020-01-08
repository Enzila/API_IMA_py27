from calendar import monthrange


class controller:
    def getdetails(self,query):
        details = query['aggregations']['type']['buckets']
        detailsdict = {}
        for a in range(details.__len__()):
            key = details[a]['key']
            if details[a]['key'] == 'null':
                key = 'comment'
            val = details[a]['doc_count']
            detailsdict[key] = val
        return detailsdict

    def getdays(self,tahun, bulan):
        return monthrange(tahun, bulan)[1]