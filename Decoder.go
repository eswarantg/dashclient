package dashclient

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
	"time"

	"github.com/eswarantg/contentconsumer"
	"github.com/eswarantg/dashreader"
	"github.com/eswarantg/statzagg"
)

//Decoder - Decodes specific component and renders
type Decoder struct {
	//External
	StatzAgg    statzagg.StatzAgg                 //Stats Agg
	RepSelector dashreader.RepresentationSelector //Representation Selector

	//Internal
	id               string                                //id of the Decoder
	reader           dashreader.Reader                     //MPD Reader
	readCtx          dashreader.ReaderContext              //Reader Context
	decoderPipe      *contentconsumer.TimedContentConsumer //Renderer
	wg               sync.WaitGroup                        //All Work done?
	source           *Mp4Source                            //Source of content
	updateChan       chan bool                             //Channel of updated AdaptationSet
	isUpdateChanOpen bool                                  //isAdaptSetChanOpen - Channel open?
	updateChanMutex  sync.Mutex                            //Mutex gaurding the channel
	streamSelector   dashreader.StreamSelector             //Choice for selecting the Right profile

	//Receiver
	datastream  chan interface{}                      //Data from Consumer
	eventstream chan contentconsumer.ConsumerEventPtr //Event from Consumer
}

//NewDecoder - Construct
func NewDecoder(id string, ss dashreader.StreamSelector) *Decoder {
	ret := new(Decoder)
	ret.id = id
	ret.RepSelector = &dashreader.MinBWRepresentationSelector{}
	ret.streamSelector = ss
	return ret
}

//Channel - Returns the channel to write urls to
func (d *Decoder) Channel() chan<- bool {
	d.updateChanMutex.Lock()
	defer d.updateChanMutex.Unlock()
	if d.isUpdateChanOpen {
		return d.updateChan
	}
	return nil
}

//CloseChannel - Close the channel
func (d *Decoder) CloseChannel() {
	d.updateChanMutex.Lock()
	defer d.updateChanMutex.Unlock()
	if !d.isUpdateChanOpen {
		return
	}
	close(d.updateChan)
	d.updateChan = nil
	d.isUpdateChanOpen = false
}

//initVideoStreamOfPlay - Cleans and sets up Video decode for selected AdaptationSet
func (d *Decoder) initStreamToPlay(reader dashreader.Reader) error {
	var readCtx dashreader.ReaderContext
	var err error

	d.cleanStreamOfPlay()

	interval := 40 * time.Millisecond //25 frames per sec = 1000 millisec / 25 = 40 msec

	readCtx, err = reader.MakeDASHReaderContext(nil, d.streamSelector, d.RepSelector)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
	}

	frate := readCtx.GetFramerate()
	interval = time.Duration(time.Duration(1000000/frate) * time.Microsecond)

	lang := "UNK"
	if len(readCtx.GetLang()) > 0 {
		lang = readCtx.GetLang()
	}
	switch readCtx.GetContentType() {
	case "video":
		d.id = d.id + "_video_"
	case "audio":
		d.id = d.id + "_audio_" + lang
	case "stpp":
		d.id = d.id + "_subtitle_" + lang
	}
	d.reader = reader
	d.readCtx = readCtx

	d.source = NewMp4Source(d.id)
	d.source.StatzAgg = d.StatzAgg
	d.datastream = make(chan interface{})
	d.eventstream = make(chan contentconsumer.ConsumerEventPtr)
	d.decoderPipe = contentconsumer.NewTimedContentConsumer(d.id, interval, 100)
	d.decoderPipe.Downstream = d.datastream
	d.decoderPipe.EventDownstream = d.eventstream
	//Open the channel for  updates
	d.updateChan = make(chan bool, 5)
	d.isUpdateChanOpen = true
	return nil
}

//cleanStreamOfPlay - Clean up existing stream of Play
func (d *Decoder) cleanStreamOfPlay() {
	if d.source != nil {
		d.source.CloseChannel()
	}
	if d.decoderPipe != nil {
		d.decoderPipe.CloseChannel()
	}
	d.wg.Wait()
}

//Run - Does actual decoding
func (d *Decoder) Run(ctx context.Context, wg *sync.WaitGroup) {
	log.Printf("Decoder::Run %v Starting...", d.id)
	defer log.Printf("Decoder::Run %v Exiting...", d.id)
	if wg != nil {
		defer wg.Done()
	}
	//Setup for Cleanup on finish
	defer d.cleanStreamOfPlay()
	//Add content handlers
	d.wg.Add(1)
	go d.handleContent(ctx, &d.wg)
	//Start the workers
	d.wg.Add(1)
	go d.decoderPipe.Run(&d.wg)
	d.wg.Add(1)
	go d.source.Run(ctx, &d.wg, d.decoderPipe.Channel())
	d.run(ctx, &d.wg)
}

//run - Main loop for executions
func (d *Decoder) run(ctx context.Context, wg *sync.WaitGroup) {
	log.Printf("Decoder::run %v Starting...", d.id)
	defer log.Printf("Decoder::run %v Exiting...", d.id)
	downstreamch := d.source.Channel()
	//Create Channel for MPD update
	mpdInternalUpdate := make(chan bool)
	//defer close(mpdInternalUpdate)

	//Setup the getURLs to listen on the channel
	urlChan := d.getURLs(ctx, wg, mpdInternalUpdate)

	for d.updateChan != nil {
		select {
		case <-ctx.Done():
			d.CloseChannel()
			continue
		case _, ok := <-d.updateChan:
			if !ok {
				d.CloseChannel()
				continue
			}
			mpdInternalUpdate <- true
		case urlData, ok := <-urlChan:
			//Give the URL to the Workers
			if !ok {
				urlChan = nil
				continue
			}
			downstreamch <- urlData
		}
	}
}

func (d *Decoder) handleContent(ctx context.Context, wg *sync.WaitGroup) {
	log.Printf("Decoder::handleContent %v Starting...", d.id)
	defer log.Printf("Decoder::handleContent %v Exiting...", d.id)
	if wg != nil {
		defer wg.Done()
	}
	for d.datastream != nil || d.eventstream != nil {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-d.datastream:
			if !ok {
				d.datastream = nil
				continue
			}
			/*
				if data != nil {
					boxes := data.([]mp4box.Box)
					if len(boxes) > 1 {
						if boxes[0].Boxtype() == "moof" {
							tboxes, err := boxes[0].GetChildrenByName("tfdt")
							if tboxes != nil && err == nil {
								var tfdtbox *mp4box.TrackFragmentBaseMediaDecodeTimeBox
								for _, tbox := range tboxes {
									tfdtbox, err = tbox.GetTrackFragmentBaseMediaDecodeTimeBox()
									if tfdtbox != nil && err == nil {
										baseMediaDecodeTime := new(uint64)
										*baseMediaDecodeTime = tfdtbox.BaseMediaDecodeTime()
									}
								}
							}
						}
					}
				}
			*/
		case de, ok := <-d.eventstream:
			if !ok {
				d.eventstream = nil
				continue
			}
			if de != nil {
				if d.StatzAgg != nil {
					values := make([]interface{}, 2)
					values[0] = de.ID
					values[1] = de.Err.Error()
					d.StatzAgg.PostEventStats(ctx, &statzagg.EventStats{
						EventClock: time.Now(),
						ID:         d.id,
						Name:       EvtClientContentAbsent,
						Values:     values,
					})
				}
			}
		}
	}
}

//getLivePointURLs - Returns a channel with urls inserted at right time for fetching
func (d *Decoder) getURLs(ctx context.Context, wg *sync.WaitGroup, updChan <-chan bool) <-chan dashreader.ChunkURL {
	var err error
	ch := make(chan dashreader.ChunkURL, 5)
	if wg != nil {
		wg.Add(1)
	}
	go func(ctx context.Context, wg *sync.WaitGroup, updChan <-chan bool, ch chan<- dashreader.ChunkURL) {
		log.Printf("Decoder::getURLs %v Starting...", d.id)
		//On exit func the channel
		defer log.Printf("Decoder::getURLs %v Exiting...", d.id)
		if wg != nil {
			defer wg.Done()
		}
		defer close(ch)
		for {
			var urlChan <-chan dashreader.ChunkURL
			urlChan = nil
			d.readCtx, err = d.reader.MakeDASHReaderContext(d.readCtx, d.streamSelector, d.RepSelector)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					log.Printf("Update Handling Failed %v", err)
				}
			} else {
				urlChan, err = d.readCtx.NextURLs(ctx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						log.Printf("Opening URLs Failed %v", err)
					}
				}
			}
			updateRequired := false
			for !updateRequired {
				//Determine live point using d.adaptSetCtx
				select {
				case <-ctx.Done():
					//Context Cancelled
					log.Printf("Decoder::getURLs %v cancelled...", d.id)
					return
				case _, ok := <-updChan:
					if !ok {
						//Input channel is closed. Exit.
						log.Printf("Decoder::getURLs %v input closed...", d.id)
						return
					}
					updateRequired = true
					break
				case chunkURL, ok := <-urlChan:
					if !ok {
						urlChan = nil
						break
					}
					ch <- chunkURL
				}
			}

		}
	}(ctx, wg, updChan, ch)
	return ch
}
