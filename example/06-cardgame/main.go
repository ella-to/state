// Command cardgame demonstrates ella.to/state with a 4-player trick-taking
// game. Each player runs in its own goroutine, waits for its turn, and
// advances the game by applying Play actions to the shared machine.
package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"ella.to/state"
)

type Suit int

const (
	Clubs Suit = iota
	Diamonds
	Hearts
	Spades
)

func (s Suit) String() string { return [...]string{"♣", "♦", "♥", "♠"}[s] }

type Card struct {
	Suit Suit
	Rank int // 2..14 (11=J, 12=Q, 13=K, 14=A)
}

func (c Card) String() string {
	names := map[int]string{11: "J", 12: "Q", 13: "K", 14: "A"}
	if n, ok := names[c.Rank]; ok {
		return fmt.Sprintf("%s%s", n, c.Suit)
	}
	return fmt.Sprintf("%d%s", c.Rank, c.Suit)
}

type Phase int

const (
	Playing Phase = iota
	Done
)

// Game is the machine's context, shared by every handler and Wait call.
type Game struct {
	Hands  [4][]Card
	Table  []Card // cards played this trick, Table[i] played by (Leader+i)%4
	Leader int    // player who led the current trick
	Turn   int    // player expected to act now
	Tricks [4]int
}

// Play is the only action: player p plays card c from their hand.
type Play struct {
	Player int
	Card   Card
}

func (g *Game) leadSuit() Suit { return g.Table[0].Suit }

func (g *Game) hasSuit(player int, s Suit) bool {
	return slices.ContainsFunc(g.Hands[player], func(c Card) bool { return c.Suit == s })
}

// play validates and applies a Play action, returning the next phase.
func (g *Game) play(p Play) (Phase, error) {
	if p.Player != g.Turn {
		return 0, fmt.Errorf("player %d: not your turn (player %d is up)", p.Player, g.Turn)
	}
	i := slices.Index(g.Hands[p.Player], p.Card)
	if i < 0 {
		return 0, fmt.Errorf("player %d does not hold %v", p.Player, p.Card)
	}
	if len(g.Table) > 0 && p.Card.Suit != g.leadSuit() && g.hasSuit(p.Player, g.leadSuit()) {
		return 0, fmt.Errorf("player %d must follow %v", p.Player, g.leadSuit())
	}

	g.Hands[p.Player] = slices.Delete(g.Hands[p.Player], i, i+1)
	g.Table = append(g.Table, p.Card)
	fmt.Printf("  player %d plays %v\n", p.Player, p.Card)

	if len(g.Table) < 4 {
		g.Turn = (g.Turn + 1) % 4
		return Playing, nil
	}

	// Trick complete: highest rank of the lead suit wins and leads the next.
	winner, best := g.Leader, g.Table[0]
	for i, c := range g.Table[1:] {
		if c.Suit == best.Suit && c.Rank > best.Rank {
			winner, best = (g.Leader+i+1)%4, c
		}
	}
	g.Tricks[winner]++
	fmt.Printf("trick to player %d (%v)\n\n", winner, best)

	g.Table = g.Table[:0]
	g.Leader, g.Turn = winner, winner
	if len(g.Hands[winner]) == 0 {
		return Done, nil
	}
	return Playing, nil
}

// player waits for its turn, picks a legal card, and applies it. It exits
// when the game reaches Done.
func player(m *state.Machine[Phase, Game], id int) {
	for {
		var card Card
		done := false
		m.Wait(func(ph Phase, g *Game) bool {
			if ph == Done {
				done = true
				return true
			}
			if g.Turn != id {
				return false
			}
			card = choose(g, id)
			return true
		})
		if done {
			return
		}
		if _, err := m.Do(Play{Player: id, Card: card}); err != nil {
			panic(err) // unreachable: Wait guaranteed the play is legal
		}
	}
}

// choose picks the first card that follows the lead suit, or the first card
// in hand when leading or out of suit.
func choose(g *Game, id int) Card {
	hand := g.Hands[id]
	if len(g.Table) > 0 {
		if i := slices.IndexFunc(hand, func(c Card) bool { return c.Suit == g.leadSuit() }); i >= 0 {
			return hand[i]
		}
	}
	return hand[0]
}

func deal() (hands [4][]Card) {
	var deck []Card
	for s := Clubs; s <= Spades; s++ {
		for r := 2; r <= 14; r++ {
			deck = append(deck, Card{s, r})
		}
	}
	rng := rand.New(rand.NewPCG(2026, 7))
	rng.Shuffle(len(deck), func(i, j int) { deck[i], deck[j] = deck[j], deck[i] })
	for p := range 4 {
		hands[p] = deck[p*5 : (p+1)*5] // 5 cards each
	}
	return hands
}

func main() {
	game := Game{Hands: deal()}
	for p, h := range game.Hands {
		fmt.Printf("player %d hand: %v\n", p, h)
	}
	fmt.Println()

	m := state.New(Playing, game)
	m.On(Playing, (*Game).play)

	// Out-of-turn and unregistered actions are rejected without advancing.
	if _, err := m.Do(Play{Player: 2, Card: Card{Spades, 14}}); err != nil {
		fmt.Printf("rejected: %v\n", err)
	}
	if _, err := m.Do("not an action"); err != nil {
		fmt.Printf("rejected: %v\n\n", err)
	}

	var wg sync.WaitGroup
	for id := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			player(m, id)
		}()
	}
	wg.Wait()

	var tricks [4]int
	m.Wait(func(_ Phase, g *Game) bool { tricks = g.Tricks; return true })
	fmt.Printf("final state: %v, tricks: %v\n", m.State(), tricks)
}
