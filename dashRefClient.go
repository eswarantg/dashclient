package dashclient

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/eswarantg/dashreader"
	"github.com/eswarantg/statzagg"
)

//Client -
type Client struct {
	Timeout          time.Duration                  //Timeout of requets to server
	StreamSelections *dashreader.StreamSelectorList //Stream Selection criteria
	StatzAgg         statzagg.StatzAgg              //statistics aggregator

	id     string     //User given name for the client
	mpdSrc *MpdSource //Mpd Source

	playCancel context.CancelFunc //cancel function to stop play
	plagWg     sync.WaitGroup     //Wait Group for joining all
	decoders   []*Decoder         //decoders
	mutex      sync.Mutex         //Mutex to gaurd modifications
}

//NewClient - Constructor
func NewClient(id string, mpdURL url.URL, ssconfigfilename string) *Client {
	var err error
	ret := &Client{}
	ret.id = id
	ret.mpdSrc = NewMpdSource(id, mpdURL)
	log.Printf("Created MpdSource for %v.", mpdURL.String())
	ret.Timeout = 5 * time.Second
	if len(ssconfigfilename) > 0 {
		ret.StreamSelections, err = dashreader.NewStreamSelectorList(ssconfigfilename)
		if err != nil {
			log.Printf("Unable to load stream selection %v", err)
			ret.StreamSelections = new(dashreader.StreamSelectorList)
		}
	} else {
		ret.StreamSelections = new(dashreader.StreamSelectorList)
	}
	return ret
}

//initStreamsForPlay - Init streams for play
func (c *Client) initStreamsForPlay(ctx context.Context) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	contentTypes := []string{"video", "audio"}
	for _, contentType := range contentTypes {
		decoder := NewDecoder(c.id, *c.StreamSelections.GetStream(contentType))
		decoder.StatzAgg = c.StatzAgg
		err := decoder.initStreamToPlay(c.mpdSrc.reader)
		if err != nil {
			log.Printf("Cannot initstream for %s :%v", contentType, err)
			return err
		}
		log.Printf("New Decoder : %v", contentType)
		c.decoders = append(c.decoders, decoder)
	}
	return nil
}

//cleanStreamsOfPlay - Cleans up any Stream playing
func (c *Client) cleanLastPlay() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.playCancel != nil {
		c.playCancel()
		c.playCancel = nil
	}
	for _, decoder := range c.decoders {
		decoder.CloseChannel()
	}
	c.decoders = []*Decoder{}
}

//handleMpdUpdateLive - handleMpdUpdate for Live Stream
func (c *Client) handleMpdUpdate(ctx context.Context) error {
	for _, decoder := range c.decoders {
		ch := decoder.Channel()
		if ch != nil {
			ch <- true
		}
	}
	return nil
}

func (c *Client) handleMpdUpdates(ctx context.Context, wg *sync.WaitGroup, updates <-chan time.Time) error {
	log.Printf("handleMpdUpdates %v Starting...", c.id)
	defer log.Printf("handleMpdUpdates %v Exiting...", c.id)
	if wg != nil {
		defer wg.Done()
	}
	defer c.cleanLastPlay()
	for {
		select {
		case <-ctx.Done():
			return nil
		case publishTime, ok := <-updates:
			if !ok {
				//Channel is closed
				return nil
			}
			if len(c.decoders) == 0 {
				if c.StatzAgg != nil {
					c.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         c.id,
						Name:       EvtNewMPD,
					})
				}
				//First time MPD
				err := c.initStreamsForPlay(ctx)
				if err != nil {
					return fmt.Errorf("Init Streams failed : %w", err)
				}
				for _, decoder := range c.decoders {
					c.plagWg.Add(1)
					go decoder.Run(ctx, &c.plagWg)
				}
				log.Printf("Finishd Starting Decoders.")
			} else {
				values := make([]interface{}, 1)
				values[0] = publishTime
				if c.StatzAgg != nil {
					c.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         c.id,
						Name:       EvtUpdMPD,
						Values:     values,
					})
				}
				//handle update mpd
				c.handleMpdUpdate(ctx)
			}
		}
	}
}

//Play - Start playing content
func (c *Client) Play(ctx context.Context) error {
	c.cleanLastPlay()
	log.Printf("Clearing Last Play - Completed.")
	ctx, c.playCancel = context.WithCancel(ctx)
	c.mpdSrc.StatzAgg = c.StatzAgg
	mpdChan := c.mpdSrc.GetMpdUpdates(ctx, &c.plagWg)
	log.Printf("MpdSource Updates requested.")
	c.plagWg.Add(1)
	go c.handleMpdUpdates(ctx, &c.plagWg, mpdChan)
	log.Printf("MpdSource Updates initiated.")
	return nil
}

//WaitAndStopAfter - Stop playing content after
// If duration is nil... it waits infinitely
func (c *Client) WaitAndStopAfter(duration *time.Duration) {
	if duration != nil {
		time.AfterFunc(*duration, func() {
			c.cleanLastPlay()
		})
	}
	log.Printf("Waiting for Play to End.")
	c.plagWg.Wait()
	log.Printf("Play Ended.")
}

//Stop - Stop playing content
func (c *Client) Stop() {
	log.Printf("Stopping Play....")
	c.cleanLastPlay()
	c.plagWg.Wait()
	log.Printf("Play Ended.")
}
