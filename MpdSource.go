package dashclient

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/eswarantg/dashreader"
	"github.com/eswarantg/statzagg"
	"github.com/tcnksm/go-httpstat"
)

//MpdSource - Setup a MPD, Get a channel of updates
type MpdSource struct {
	//External
	StatzAgg statzagg.StatzAgg //StatsAgg
	Timeout  time.Duration     //Timeout in fetch
	//Internal
	id           string            //id of the element
	mpdURL       url.URL           //url to playback
	reader       dashreader.Reader // Reader
	minUpdPeriod time.Duration     //Minimum Update Duration
}

//NewMpdSource - New MPD
func NewMpdSource(id string, mpdURL url.URL) *MpdSource {
	ret := new(MpdSource)
	ret.mpdURL = mpdURL
	ret.id = id
	return ret
}

//GetMpdUpdates - Get Latest MPD and check for updates
func (s *MpdSource) GetMpdUpdates(ctx context.Context, wg *sync.WaitGroup) <-chan time.Time {
	//Create a channel for MPD sending
	ch := make(chan time.Time, 5)
	if wg != nil {
		wg.Add(1)
	}
	//Start a go routine to read MPD and publish to the channel
	//once context is cancelled, it will close the channel
	go func(ctx context.Context, wg *sync.WaitGroup) {
		log.Printf("GetMpdUpdates %v Starting...", s.id)
		defer log.Printf("GetMpdUpdates %v Exiting...", s.id)
		if wg != nil {
			defer wg.Done()
		}

		var newMpd *dashreader.MPDtype
		var err error

		//close channel on exit
		defer close(ch)

		delayBetweenMpdFetch := 2 * time.Second
		duration := 0 * time.Second
		//log.Printf("MpdSource Starting loop")
		//till ctx is cancelled
		for {
			select {
			case <-ctx.Done():
				log.Printf("MpdSource Exiting Context Cancelled")
				return
			case <-time.After(duration):
				//log.Printf("MpdSource Fetching MPD.")
				duration = delayBetweenMpdFetch
				newMpd, err = s.getMpd(ctx)
				if err != nil {
					//Error getting MPD
					values := make([]interface{}, 1)
					values[0] = s.mpdURL.String()
					s.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         s.id,
						Name:       EvtMPDFetchFail,
						Err:        err,
						Values:     values,
					})
					continue
				}
				if s.reader == nil {
					factory := dashreader.ReaderFactory{}
					s.reader, err = factory.GetDASHReader(s.id, s.mpdURL.String(), newMpd)
					if err != nil {
						s.reader = nil
						//Error getting MPD
						s.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
							EventClock: time.Now(),
							ID:         s.id,
							Name:       EvtMPDFetchFail,
							Err:        err,
						})
						continue
					}
				} else {
					var updated bool
					updated, err = s.reader.Update(newMpd)
					if err != nil {
						log.Printf("Error updating MPD:%v", err)
						continue
					}
					if !updated {
						continue
					}
					ch <- newMpd.PublishTime
				}
				s.minUpdPeriod, err = dashreader.ParseDuration(newMpd.MinimumUpdatePeriod)
				if err != nil {
					s.minUpdPeriod = 2 * time.Second
				}
				if s.minUpdPeriod == 0 {
					s.minUpdPeriod = 1
				}
				delayBetweenMpdFetch = s.minUpdPeriod
				duration = delayBetweenMpdFetch
			}
		}
	}(ctx, wg)

	return ch
}

//getMpd - Fetch MPD from the URL
func (s *MpdSource) getMpd(ctx context.Context) (*dashreader.MPDtype, error) {
	var req *http.Request
	var err error
	statz := &statzagg.HTTPClientStatz{}
	statz.ID = s.id
	statz.URL = s.mpdURL.String()

	client := &http.Client{
		Timeout: s.Timeout,
	}
	req, err = http.NewRequest(http.MethodGet, s.mpdURL.String(), nil)
	if err != nil {
		statz.Err = fmt.Errorf("NewRequest Fail: %w", err)
		return nil, statz.Err
	}
	req = req.WithContext(ctx)
	// Create go-httpstat powered context and pass it to http.Request
	sCtx := httpstat.WithHTTPStat(req.Context(), &statz.Result)
	req = req.WithContext(sCtx)

	//Start a clock for overall duration compute
	statz.BegClock = time.Now()
	defer func(s *MpdSource) {
		statz.EndClock = time.Now()
		if s.StatzAgg != nil {
			s.StatzAgg.PostHTTPClientStats(ctx, statz)
		}
	}(s)

	res, err := client.Do(req)
	if err != nil {
		statz.Err = fmt.Errorf("client.Do Fail: %w", err)
		return nil, statz.Err
	}
	defer res.Body.Close()
	err = statz.ReadHTTPHeader(&res.Header)
	if err != nil {
		statz.Err = fmt.Errorf("statz.ReadHTTPHeader Fail: %w", err)
		return nil, statz.Err
	}
	statz.Status = res.StatusCode
	//Check for error
	if res.StatusCode != http.StatusOK {
		//Error
		statz.Err = fmt.Errorf("HTTP:Err:%v", res.StatusCode)
		return nil, statz.Err
	}
	mpd, err := dashreader.ReadMPDFromStream(res.Body)
	if err != nil {
		statz.Err = fmt.Errorf("ReadMPDFromStream Fail: %w", err)
		return nil, err
	}
	return mpd, nil
}
