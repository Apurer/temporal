// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTMARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package stamp

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	DebugActID = ActID("<debugActID>")
)

type (
	action interface {
		String() string
	}
	ActionParams struct {
		Payload any
		ActID   ActID
		ReqOnly bool
	}
	routableAction interface {
		action
		Route() string
		ID() ActID
		GetValidationErrors() []string
	}
	ActID                         ID
	ActionActor[MA modelAccessor] struct {
		actor MA
	}
	ActorModel[MA modelAccessor] struct {
		actor MA
	}
	actorModel[MA modelAccessor] interface {
		modelAccessor
		getModelAccessor() MA
		OnAction(context.Context, ActionParams) error // implemented by user
	}
	ActionTarget[M modelAccessor] struct {
		mdl M
	}
	ActionTargetAsync[M modelAccessor] struct {
		mdl M
	}
	syncActionGen[A actorModel[AMA], AMA modelAccessor, T modelAccessor, P payload] interface {
		Generator[P]
		GetActor() AMA
		getTarget() T
	}
	asyncActionGen[A actorModel[AMA], AMA modelAccessor, T modelAccessor, P payload] interface {
		Generator[P]
		GetActor() AMA
		getTarget() T
	}
	payload       interface{}
	actOption     interface{} // TODO: add marker
	actIdDebugOpt struct {
		id ActID
	}
	ignoreErrOpt  struct{}
	actReqOnlyOpt struct{}
)

func NewActorModel[AM modelAccessor](mdl AM) ActorModel[AM] {
	return ActorModel[AM]{actor: mdl}
}

func newSeed(values ...any) int {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", values)))
	return int(h.Sum32())
}

func WithDebugActID() actOption {
	if _, ok := os.LookupEnv("CI"); ok {
		panic("WithDebugActID is not available in CI, only for local debugging")
	}
	return actIdDebugOpt{id: DebugActID}
}

func IgnoreErr() actOption {
	return ignoreErrOpt{}
}

// Act sends an action, as created by the action generator, from the actor to the System-under-Test (SUT).
// It blocks until the SUT's response is returned; or times out. It fails if the action was not routed
// to the expected target, as defined by the action generator.
func Act[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	TMA modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG syncActionGen[A, AMA, TMA, P], // the trigger type
](actor A, actionGen AG, opts ...actOption) TMA {
	res, err := act[A, AMA, TMA, P, AG](actor, actionGen, opts...)
	if err != nil {
		actor.getModel().getEnv().Fatal(err.Error())
	}
	return res
}

// ActStart works like Act but returns immediately once the targeted model has received the
// generated action, instead of waiting for the System-under-Test's response.
// TODO: enforce compile-type check to match Action
// TODO: MUST: any access to the returned model must first verify that the _response_ was received! to prevent flakiness
func ActStart[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	TMA modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG asyncActionGen[A, AMA, TMA, P],
](actor A, actionGen AG, opts ...actOption) TMA {
	res, err := act[A, AMA, TMA, P, AG](actor, actionGen, append(opts, actReqOnlyOpt{})...)
	if err != nil {
		actor.getModel().getEnv().Fatal(err.Error())
	}
	return res
}

func ActAsync[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	TMA modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG syncActionGen[A, AMA, TMA, P], // the trigger type
](actor A, actionGen AG, opts ...actOption) Future[TMA] {
	f := newFuture[TMA](actor.getModel().getEnv().getTestEnv(), func() (TMA, error) {
		return act(actor, actionGen, append(opts)...)
	})
	return f
}

func act[
	A actorModel[AMA], // the actor that is triggering the action
	AMA modelAccessor, // the underlying model of the actor
	TMA modelAccessor, // the target model to receive the action
	P payload, // the payload
	AG asyncActionGen[A, AMA, TMA, P],
](actor A, actionGen AG, opts ...actOption) (TMA, error) {
	env := actor.getModel().getEnv()
	tenv := env.getTestEnv()

	var ignoreErr bool
	actParams := ActionParams{ActID: ActID("act:" + uuid.NewString())} // prefix identifies actions from a scenario
	for _, opt := range opts {
		switch o := opt.(type) {
		case actIdDebugOpt:
			actParams.ActID = o.id
		case actReqOnlyOpt:
			actParams.ReqOnly = true
		case ignoreErrOpt:
			ignoreErr = true
		default:
			env.Fatal(fmt.Sprintf("unknown option: %T", opt))
		}
	}

	// validate action generator
	if err := validator.Struct(actionGen); err != nil {
		env.Fatal(fmt.Sprintf("action generator '%v' is invalid: %v", actionGen, err))
	}

	// copy action generator and assign actor
	// TODO: check that it wasn't set by user
	copyVal := reflect.New(reflect.TypeOf(actionGen))
	copyVal.Elem().Set(reflect.ValueOf(actionGen))
	copyVal.Elem().FieldByName("ActionActor").
		Set(reflect.ValueOf(ActionActor[AMA]{actor: actor.getModelAccessor()}))
	newActGen := copyVal.Elem().Interface().(syncActionGen[A, AMA, TMA, P])

	// generate the action
	// TODO: use type of payload to change the seed; that gives actions more unique IDs
	actParams.Payload = newActGen.Next(tenv.genContext())

	// setup trigger callback and action monitor
	// TODO: the monitoring/logging for the action should be part of the action itself
	actMonitor := newActionLog[TMA](env)
	actMonitor.start(actParams.ActID)

	env.Info(fmt.Sprintf("Sending '%v'",
		simpleSpew.Sdump(actParams.Payload)), actionIdTag(actParams.ActID))

	// TODO: configurable timeout
	ctx := tenv.Context(defaultActionTimeout)
	timeout, _ := ctx.Deadline()

	var sendErr atomic.Pointer[error]
	var completed atomic.Bool
	send := func() {
		err := actor.OnAction(ctx, actParams)
		if err != nil {
			if !ignoreErr {
				err = fmt.Errorf("failed to send '%#v': %v", actParams.Payload, err)
				sendErr.Store(&err)
			} else {
				// TODO: improve
				env.Warn(fmt.Sprintf("failed to send '%#v': %v", actParams.Payload, err))
			}
		}
		completed.Store(true)
	}
	if actParams.ReqOnly {
		go send()
	} else {
		send()
		if err := sendErr.Load(); err != nil {
			var zero TMA
			return zero, *err
		}
	}

	for {
		if err := sendErr.Load(); err != nil {
			var zero TMA
			return zero, *err
		}
		if time.Now().After(timeout) {
			break
		}
		if actParams.ReqOnly && actMonitor.matched() {
			break // async exits asap
		}
		if !actParams.ReqOnly && completed.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// verify that the action arrived at the target
	res := actMonitor.stop()
	if res == nil {
		var zero TMA
		return zero, actMonitor.error(actor, newActGen)
	}
	return *res, nil
}

func (tt ActionTarget[M]) getTarget() M {
	return tt.mdl
}

func (tt ActionTargetAsync[M]) getTarget() M {
	return tt.mdl
}

func (ta ActionActor[A]) GetActor() A {
	return ta.actor
}

func (a *ActorModel[MA]) getModel() *internalModel {
	return a.actor.getModel()
}

func (a *ActorModel[MA]) GetID() ID {
	return a.actor.GetID()
}

func (a *ActorModel[MA]) getModelAccessor() MA {
	return a.actor
}
