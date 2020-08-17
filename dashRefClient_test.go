package dashclient_test

import (
	"context"
	"dashclient"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/eswarantg/statzagg"
)

func playContent(t *testing.T, urlStr string, name string, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	startzAgg := statzagg.NewLogStatzAgg(os.Stdout)
	mpdURL, err := url.Parse(urlStr)
	if err != nil {
		t.Logf("\n %v : %v", mpdURL, err)
		return
	}
	dc := dashclient.NewClient(name, *mpdURL, "./main/streamselection.json")
	dc.StatzAgg = startzAgg
	err = dc.Play(context.TODO())
	if err != nil {
		t.Errorf("Play returned error for url %s err:%s", mpdURL, err)
		return
	}
	duration := 10 * time.Second
	dc.WaitAndStopAfter(&duration)
}

func TestNewClient(t *testing.T) {
	var wg sync.WaitGroup
	N := 10
	for i := 1; i <= N; i++ {
		wg.Add(1)
		go playContent(t,
			"https://livesim.dashif.org/livesim/segtimeline_1/testpic_2s/Manifest.mpd",
			"client"+strconv.Itoa(i), &wg)
	}
	wg.Wait()
}
