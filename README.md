# WARPT
### Wrong Approach Ruby Parsing Test

I was excited to get a code test from an agile coaching business. I did not have much Ruby experience, but I have many years experience writing parsing code, and so... The code test was like a homework assignment -- it was to parse three files featuring different formats and delimiters, sort the aggregate results three ways, and export the sorted rows in a single file.

I could not bring myself to do it -- write a parser that did not check the data rows for the export delimiter, and such. So I coded something closer to what might be used in reality.

Honestly, I am still not sure how this would be unit tested, and it will be a while before I write fluent *idiomatic* Ruby. For example, this

    if !$test_mode then
      warpt_run()
    end

should probably have been *this*.

    if $0 == __FILE__
      warpt_run()
    end

Despite writing high level tests, and solving many problems not posed in the original test -- the response was rather harsh. I can understand it somewhat, considering how I would feel if expecting a simpleton hack, and getting 500 lines of code.

But it reminds me of how a professor chided me, when I was caught smirking at his momentary lapse... *You're not that good.*

I have coaching and teaching experience, so I was really excited by the opportunity. This kind of response though, makes me wonder if I am too dimwitted to be employable in this arid terrain of TDD-Agile gurus and zealots.

On my pile is "Refactoring in Ruby", and I am sure it will help me become a better Rubyist. But this parser seems lucid and easy to maintain, and I included plenty of *why* in comments.

Please do not troll, but drop me a line if there is anything particularly egregious about how I wrote this program. I would love to learn from similar projects that are deemed well-done. I learned Ruby primarily from Black "The Well-Grounded Rubyist", and I am working through "Agile Web Development with Rails". I have also read Dix "Service-Oriented Design with Ruby and Rails".

The code has been slightly modified to export three files instead of a single composite file. Essentially, this is the code that elicited the following feedback...

### Warpt Feedback

    - The solution creates intermediate files, has huge methods, terrible tests, unreadable code.
    - Puts tons of method definitions in a before filter in the tests.  Very odd.
    - Redirects standard out to clean up test output
    - Tests contain a ton of duplication
    - Each method is very noisy and hard to read
    - Tests parse for Stack Traces being output to the redirected stdout.


### File Organization
    warpt/
        data_batch/
            test_warpt_batch/
                data_parsed/
                data_rejected/
                log/
                results/
                staging/
        data_drop/
        README.md
        test_files/
            example_run/
            reference_data/
                comma.txt
                pipe.txt
                semicolon.txt
            reference_issues/
                data_issue_date.txt
                data_issue_name.txt
                empty.txt
                format_error.txt
                format_unknown.txt
            reference_result/
                dob-sort.txt
                gender-sort.txt
                reverse-name-sort.txt
        warpt_test.rb
        warpt.rb



### Environment
    Macintosh-2:~] direwolf% ruby --version
    ruby 1.9.2p290 (2011-07-09 revision 32553) [x86_64-darwin10]


### Example Production Run
    [Macintosh-2:~/Desktop/warpt] direwolf% ruby /Users/direwolf/Desktop/warpt/warpt.rb
    warpt .. 2011-10-26-163730 .. Script Launch
    warpt .. 2011-10-26-163730 .. Processing File [1/3] :: comma.txt
    warpt .. 2011-10-26-163730 .. Processing File [2/3] :: pipe.txt
    warpt .. 2011-10-26-163730 .. Processing File [3/3] :: semicolon.txt
    warpt .. 2011-10-26-163730 .. Processing Complete


### Example Test Run
    [Macintosh-2:~/Desktop/warpt] direwolf% ruby /Users/direwolf/Desktop/warpt/warpt_test.rb
    Loaded suite /Users/direwolf/Desktop/warpt/warpt_test
    Started

    .......
    Finished in 0.100984 seconds.

    7 tests, 24 assertions, 0 failures, 0 errors, 0 skips
